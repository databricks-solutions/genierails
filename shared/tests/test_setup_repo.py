"""make setup REPO=<url>: GenieRails prepares the customer git repo (option A).

A fresh GenieRails clone (origin at GenieRails) run with REPO= keeps
GenieRails as the ``genierails`` remote and makes origin the customer's repo;
re-running with the same REPO changes nothing, and a different REPO once
origin exists is refused before anything is written (no env file, no remote
change). Without REPO a fresh clone becomes a local-only repo (only the
``genierails`` upgrade remote, no origin), so only the git binary is needed.
Nothing is ever pushed: REPO is a local bare
repo here and must stay empty. Real make setup runs in temporary repos, with
GIT_CONFIG_GLOBAL=/dev/null so the developer's global hooks and signing don't
apply.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
sys.path.insert(0, str(SHARED))

from scripts import customer_gitignore as cg  # noqa: E402
from scripts import release_gate as rg  # noqa: E402
from scripts import setup_repo as sr  # noqa: E402

UPSTREAM = "https://github.com/databricks-solutions/genierails.git"
CLOUDS = ("aws", "azure")


def _env(**extra):
    drop = ("MAKEFLAGS", "MAKELEVEL", "ENV", "GITIGNORE", "REPO", "CI", "GENIERAILS_ALLOW_CI_APPLY",
            "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI")
    env = {k: v for k, v in os.environ.items() if k not in drop}
    env.update(PIP_NO_INDEX="1", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1", **extra)
    return env


def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True,
                          env=_env(), check=True).stdout


def _remotes(repo):
    return dict(line.split()[:2] for line in _git(repo, "remote", "-v").splitlines() if "(fetch)" in line)


def _clone(tmp_path, cloud="aws", origin=UPSTREAM):
    """A checkout shaped like a fresh git clone of GenieRails (no network)."""
    repo = tmp_path / "genierails"
    (repo / cloud).mkdir(parents=True)
    (repo / "shared").mkdir()  # setup manages git only at a GenieRails checkout root
    _git(repo, "init", "-q")
    if origin:
        _git(repo, "remote", "add", "origin", origin)
    return repo


def _customer_repo(tmp_path, name="gov.git"):
    bare = tmp_path / name
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True, env=_env())
    return str(bare)


def _setup(cloud_root, *args, env=None):
    return subprocess.run(
        ["make", "--no-print-directory", "setup", *args, f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"],
        cwd=ROOT / cloud_root.name, text=True, capture_output=True, env=env or _env())


def _snapshot(repo):
    """Every file under repo (outside .git) with its bytes, plus the git config."""
    files = {p.relative_to(repo): p.read_bytes() for p in repo.rglob("*")
             if p.is_file() and ".git" not in p.relative_to(repo).parts}
    return files, (repo / ".git" / "config").read_bytes()


def _no_refs(bare):
    return _git(bare, "for-each-ref") == ""


def _commit_all(repo, message="commit"):
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-q", "-m", message)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_fresh_clone_with_repo_renames_origin_adds_repo_and_pushes_nothing(cloud, tmp_path):
    repo = _clone(tmp_path, cloud)
    url = _customer_repo(tmp_path)
    result = _setup(repo / cloud, "ENV=dev", f"REPO={url}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM, "origin": url}
    assert "Git: renamed remote origin -> genierails" in result.stdout
    assert f"Git: added remote origin = {url}" in result.stdout
    assert "origin is ready; setup may offer to commit and push its safe files at the end" in result.stdout
    assert _no_refs(url)
    # origin is now the customer's, so the same setup run wrote the .gitignore block.
    assert cg.BEGIN in (repo / cloud / ".gitignore").read_text()
    assert (repo / cloud / "envs/dev/env.auto.tfvars").is_file()


def test_rerun_with_the_same_repo_is_a_no_op(tmp_path):
    repo = _clone(tmp_path)
    url = _customer_repo(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", f"REPO={url}").returncode == 0
    before = _snapshot(repo)
    for same in (url, url + "/", url.removesuffix(".git")):
        again = _setup(repo / "aws", "ENV=dev", f"REPO={same}")
        assert again.returncode == 0, again.stdout + again.stderr
        assert "nothing to change" in again.stdout and "renamed" not in again.stdout
        assert _snapshot(repo) == before
    # No REPO at all on a customer repo: remotes untouched, nothing printed about git.
    plain = _setup(repo / "aws", "ENV=dev")
    assert plain.returncode == 0 and "Git:" not in plain.stdout
    assert _snapshot(repo) == before
    assert _no_refs(url)


@pytest.mark.parametrize("env,jobs", [("dev", ()), ("prod", ()), ("prod", ("-j8",))])
def test_conflicting_repo_is_refused_before_anything_is_written(env, jobs, tmp_path):
    repo = _clone(tmp_path)
    url = _customer_repo(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", f"REPO={url}").returncode == 0
    before = _snapshot(repo)
    other = _customer_repo(tmp_path, "other.git")
    result = _setup(repo / "aws", *jobs, f"ENV={env}", f"REPO={other}")
    assert result.returncode != 0
    assert f"origin already points at {url}, not {other}" in result.stderr
    assert "never repoints" in result.stderr and f"git remote set-url origin {other}" in result.stderr
    assert "leave REPO= off" in result.stderr.replace("Leave", "leave")
    assert _snapshot(repo) == before  # prod's env dir was not created, the .gitignore is unchanged
    assert not (repo / "aws/envs/prod").exists()
    assert "=== Setup" not in result.stdout


def test_refused_repo_on_a_fresh_clone_writes_nothing(tmp_path):
    repo = _clone(tmp_path)
    _git(repo, "remote", "add", "genierails", "https://example.com/mirror.git")
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={_customer_repo(tmp_path)}")
    assert result.returncode != 0
    assert "a genierails remote already exists" in result.stderr
    assert _snapshot(repo) == before and not (repo / "aws/envs").exists()


@pytest.mark.parametrize("bad", [UPSTREAM, "git@github.com:databricks-solutions/genierails.git", "-c x", "a b"])
def test_repo_must_be_your_own_url(bad, tmp_path):
    repo = _clone(tmp_path)
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={bad}")
    assert result.returncode != 0 and "ERROR: make setup REPO=" in result.stderr
    assert _snapshot(repo) == before and not (repo / "aws/envs").exists()


def test_the_scheduled_governance_guard_still_runs_first_and_no_remote_changes(tmp_path):
    repo = _clone(tmp_path)
    dev = repo / "aws/envs/dev"
    dev.mkdir(parents=True)
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\nenable_scheduled_governance = true\n')
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={_customer_repo(tmp_path)}")
    assert result.returncode != 0
    assert "enable_scheduled_governance = true is not supported" in result.stderr
    assert _snapshot(repo) == before and _remotes(repo) == {"origin": UPSTREAM}


def test_no_repo_makes_a_fresh_clone_local_only(tmp_path):
    repo = _clone(tmp_path)
    result = _setup(repo / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM}
    assert "local-only repo" in result.stdout and "make setup ENV=dev REPO=<url>" in result.stdout
    assert cg.BEGIN in (repo / "aws/.gitignore").read_text()
    before = _snapshot(repo)
    again = _setup(repo / "aws", "ENV=dev")
    assert again.returncode == 0 and "Git:" not in again.stdout and _snapshot(repo) == before
    # Adding the customer repo later only adds origin.
    url = _customer_repo(tmp_path)
    later = _setup(repo / "aws", "ENV=dev", f"REPO={url}")
    assert later.returncode == 0, later.stdout + later.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM, "origin": url} and "renamed" not in later.stdout
    assert _no_refs(url)


def test_setup_without_repo_leaves_provenance_on_committed_local_main(tmp_path, monkeypatch):
    for key in ("CI", "TF_BUILD", "JENKINS_URL", "GITLAB_CI", "BUILDKITE", "CIRCLECI",
                "GENIERAILS_ALLOW_CI_APPLY"):
        monkeypatch.delenv(key, raising=False)
    repo = _clone(tmp_path)
    result = _setup(repo / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    _git(repo, "branch", "-M", "main")
    _commit_all(repo, "setup")
    prov = rg.account_provenance(repo / "aws" / "envs" / "account")
    assert prov["remote"] == "local main"
    assert prov["remote_ref"] == "refs/heads/main"
    assert prov["local_only"] is True


@pytest.mark.parametrize("opt_out", [("GITIGNORE=off",), ()])
def test_no_repo_leaves_remotes_alone_for_contributors_and_ci(opt_out, tmp_path):
    repo = _clone(tmp_path)
    env = None if opt_out else _env(CI="true")
    result = _setup(repo / "aws", "ENV=dev", *opt_out, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"origin": UPSTREAM} and "Git:" not in result.stdout


@pytest.mark.parametrize("exported", ["acme/gov", "git@example.com:acme/gov.git"])
def test_an_exported_shell_repo_is_ignored(exported, tmp_path):
    """Only a command-line REPO counts (bootstrap-pipelines' REPO=<owner>/<name> may be exported)."""
    repo = _clone(tmp_path)
    result = _setup(repo / "aws", "ENV=dev", "GITIGNORE=off", env=_env(REPO=exported))
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"origin": UPSTREAM}
    # Without GITIGNORE=off the fresh clone turns local-only, and still no origin comes from the export.
    result = _setup(repo / "aws", "ENV=dev", env=_env(REPO=exported, GENIERAILS_SETUP_REPO=exported))
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM}


def test_a_repo_inherited_through_makeflags_counts(tmp_path):
    """make can't tell MAKEFLAGS=REPO=... from the command line; the owner accepts it as an opt-in."""
    repo = _clone(tmp_path)
    url = _customer_repo(tmp_path)
    result = _setup(repo / "aws", "ENV=dev", env=_env(MAKEFLAGS=f"REPO={url}"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM, "origin": url}


def test_outside_git_repo_initialises_a_repo_on_main(tmp_path):
    root = tmp_path / "genierails"
    (root / "aws").mkdir(parents=True)
    (root / "shared").mkdir()
    url = _customer_repo(tmp_path)
    result = _setup(root / "aws", "ENV=dev", f"REPO={url}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(root) == {"origin": url}
    assert _git(root, "symbolic-ref", "HEAD").strip() == "refs/heads/main"
    assert cg.BEGIN in (root / "aws/.gitignore").read_text()
    assert _no_refs(url)


def test_outside_git_without_repo_still_only_prints_the_hint(tmp_path):
    root = tmp_path / "genierails"
    (root / "aws").mkdir(parents=True)
    result = _setup(root / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (root / ".git").exists() and "Git:" not in result.stdout


def test_plan_layouts():
    url = "git@example.com:acme/gov.git"
    assert sr.plan({"origin": UPSTREAM}, url) == [("rename", "origin", "genierails"), ("add", "origin", url)]
    assert sr.plan({"genierails": UPSTREAM}, url) == [("add", "origin", url)]
    assert sr.plan({}, url) == [("add", "origin", url)]
    assert sr.plan(None, url) == [("init",), ("add", "origin", url)]
    assert sr.plan({"origin": url + "/", "genierails": UPSTREAM}, url) == []
    for same in ("https://github.com/acme/gov", "https://github.com/acme/gov.git/", "ssh://git@github.com/acme/gov.git",
                 "git@GitHub.com:acme/gov"):
        assert sr.plan({"origin": "git@github.com:acme/gov.git"}, same) == [], same
    assert sr.plan({"origin": "file:///srv/gov.git"}, "/srv/gov") == []
    with pytest.raises(sr.Refused):
        sr.plan({"origin": "git@github.com:acme/gov.git"}, "https://github.com/acme/other")
    for ok in ("https://h.example/a/b.git", "ssh://git@h.example:2222/a/b", "git@h.example:a/b.git",
               "h.example:a/b", "file:///srv/b.git", "/srv/b.git"):
        sr.validate(ok)
    assert sr.plan({"origin": UPSTREAM}, "") == [("rename", "origin", "genierails")]
    assert sr.plan({"origin": UPSTREAM}, "", auto=False) == []
    assert sr.plan({"origin": UPSTREAM, "genierails": UPSTREAM}, "") == []
    # Not a fresh clone: a contributor's fork or upstream remote next to origin.
    assert sr.plan({"origin": UPSTREAM, "upstream": UPSTREAM}, "") == []
    assert sr.plan({"origin": UPSTREAM, "fork": "https://github.com/someone/genierails.git"}, "") == []
    assert sr.plan({"origin": UPSTREAM, "fork": "https://github.com/someone/genierails.git"}, url) == [
        ("rename", "origin", "genierails"), ("add", "origin", url)]
    assert sr.plan({"origin": url}, "") == [] and sr.plan(None, "") == []
    for remotes in ({"origin": "https://example.com/x.git"}, {"origin": UPSTREAM, "genierails": UPSTREAM}):
        with pytest.raises(sr.Refused):
            sr.plan(remotes, url)


@pytest.mark.parametrize("repo_arg", [
    "REPO=$$(printf pwned > {marker})",      # Codex's repro: a make-escaped $(...) reaching the shell
    "REPO=a`printf pwned > {marker}`b",
    "REPO=https://example.com/x;printf pwned > {marker}",
    "REPO=https://example.com/x\nprintf pwned > {marker}",
    'REPO=x" ; printf pwned > {marker} ; "',
])
def test_repo_never_reaches_the_shell(repo_arg, tmp_path):
    repo = _clone(tmp_path)
    marker = tmp_path / "marker"
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", repo_arg.format(marker=marker))
    assert not marker.exists(), result.stdout + result.stderr
    assert result.returncode != 0 and "ERROR: make setup REPO=" in result.stderr
    assert _snapshot(repo) == before and _remotes(repo) == {"origin": UPSTREAM}
    assert not (repo / "aws/envs").exists()


@pytest.mark.parametrize("bad", ["ext::sh -c touch% /tmp/x", "ext::sh", "fd::17", "fd::0,1/x", "foo::bar",
                                 "http://example.com/x.git", "git://example.com/x.git", "ftp://example.com/x",
                                 "https://example.com/x\x07", "relative/path", "-uhttps://x"])
def test_dangerous_or_unknown_transports_are_refused(bad, tmp_path):
    repo = _clone(tmp_path)
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={bad}")
    assert result.returncode != 0 and "ERROR: make setup REPO=" in result.stderr
    assert _snapshot(repo) == before and not (repo / "aws/envs").exists()
    with pytest.raises(sr.Refused):
        sr.validate(bad)


def _outer(tmp_path, origin="https://example.com/outer.git"):
    """Codex's repro: GenieRails vendored into an unrelated repo at outer/vendor/genierails."""
    outer = tmp_path / "outer"
    gr = outer / "vendor" / "genierails"
    (gr / "aws").mkdir(parents=True)
    (gr / "shared").mkdir()
    _git(outer, "init", "-q")
    _git(outer, "remote", "add", "origin", origin)
    return outer, gr


def test_nested_inside_another_repo_with_repo_is_refused_and_touches_nothing(tmp_path):
    outer, gr = _outer(tmp_path)
    before = _snapshot(outer)
    result = _setup(gr / "aws", "ENV=dev", f"REPO={_customer_repo(tmp_path)}")
    assert result.returncode != 0
    assert f"is inside the git repo at {outer.resolve()}" in result.stderr
    assert _snapshot(outer) == before and _remotes(outer) == {"origin": "https://example.com/outer.git"}
    assert not (gr / ".git").exists() and not (gr / "aws/envs").exists()


def test_nested_inside_a_genierails_origin_repo_without_repo_renames_nothing(tmp_path):
    outer, gr = _outer(tmp_path, origin=UPSTREAM)
    result = _setup(gr / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(outer) == {"origin": UPSTREAM} and "Git:" not in result.stdout


def test_git_location_variables_never_redirect_the_change(tmp_path):
    outer, _ = _outer(tmp_path / "o")
    repo = _clone(tmp_path)
    url = _customer_repo(tmp_path)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={url}",
                    env=_env(GIT_DIR=str(outer / ".git"), GIT_WORK_TREE=str(outer)))
    assert _remotes(outer) == {"origin": "https://example.com/outer.git"}
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"genierails": UPSTREAM, "origin": url}


def test_a_root_without_shared_is_refused(tmp_path):
    root = tmp_path / "somewhere"
    (root / "aws").mkdir(parents=True)
    result = _setup(root / "aws", "ENV=dev", f"REPO={_customer_repo(tmp_path)}")
    assert result.returncode != 0 and "doesn't hold GenieRails' shared/" in result.stderr
    assert not (root / ".git").exists() and not (root / "aws/envs").exists()


@pytest.mark.parametrize("other", ["https://github.com/acme/gov/", "ssh://git@github.com/acme/gov",
                                   "git@GitHub.com:acme/gov"])
def test_hosted_ssh_and_https_forms_of_origin_are_the_same_repo(other, tmp_path):
    repo = _clone(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", "REPO=git@github.com:acme/gov.git").returncode == 0
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={other}")
    assert result.returncode == 0 and "nothing to change" in result.stdout, result.stdout + result.stderr
    assert _snapshot(repo) == before


@pytest.mark.parametrize("origin,other", [
    ("git@host.example:/srv/gov.git", "git@host.example:srv/gov.git"),   # Codex: absolute vs home-relative
    ("git@host.example:srv/gov.git", "git@host.example:/srv/gov.git"),
    ("git@host.example:/srv/gov.git", "ssh://git@host.example/srv/gov.git"),  # same absolute path, other form
    ("ssh://git@host.example/srv/gov.git", "git@host.example:/srv/gov.git"),
    ("git@host.example:acme/gov.git", "https://host.example/acme/gov"),   # unknown host: forms stay apart
    ("git@github.com:/acme/gov.git", "https://github.com/acme/gov"),       # hosted, but scp with a leading /
])
def test_different_paths_or_forms_on_an_unknown_host_are_refused(origin, other, tmp_path):
    repo = _clone(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", f"REPO={origin}").returncode == 0
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=prod", f"REPO={other}")
    assert result.returncode != 0 and "nothing to change" not in result.stdout
    assert f"origin already points at {origin}, not {other}" in result.stderr
    assert _snapshot(repo) == before and not (repo / "aws/envs/prod").exists()


@pytest.mark.parametrize("url", ["git@host.example:/srv/gov.git", "git@host.example:srv/gov",
                                 "ssh://git@host.example:2222/srv/gov.git", "https://host.example/acme/gov"])
def test_an_identical_unknown_host_url_is_a_no_op(url, tmp_path):
    repo = _clone(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", f"REPO={url}").returncode == 0
    before = _snapshot(repo)
    for same in (url, url + "/", url.removesuffix(".git")):
        result = _setup(repo / "aws", "ENV=dev", f"REPO={same}")
        assert result.returncode == 0 and "nothing to change" in result.stdout, result.stdout + result.stderr
        assert _snapshot(repo) == before


@pytest.mark.parametrize("bad", ["ssh://-oProxyCommand=touch/tmp/pwn/x", "git@-oProxyCommand:x", "ssh://user@-host/x",
                                 "git@host:-x", "ssh://-user@host/x", "-oProxyCommand=x:y", "ssh://host/a/-b",
                                 "https://-h.example/x", "ssh://@host/x", "ssh://h_x.example/x",
                                 "ssh://host:0/x", "ssh://host:99999/x", "https://u:pw@h.example/x", "https://h.example/"])
def test_option_like_or_malformed_hosts_users_and_paths_are_refused(bad, tmp_path):
    repo = _clone(tmp_path)
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev", f"REPO={bad}")
    assert result.returncode != 0 and "ERROR: make setup REPO=" in result.stderr, result.stdout + result.stderr
    assert _snapshot(repo) == before and _remotes(repo) == {"origin": UPSTREAM}
    assert not (repo / "aws/envs").exists()
    with pytest.raises(sr.Refused):
        sr.validate(bad)


@pytest.mark.parametrize("origin,other", [
    ("ssh://git@example.com:2222/acme/gov.git", "ssh://git@example.com:22/acme/gov.git"),
    ("ssh://alice@example.com/acme/gov.git", "ssh://bob@example.com/acme/gov.git"),
    ("ssh://git@example.com:2222/acme/gov.git", "git@example.com:acme/gov.git"),
    ("https://example.com:8443/acme/gov", "https://example.com/acme/gov"),
])
def test_a_different_port_or_user_is_not_the_same_repo(origin, other, tmp_path):
    repo = _clone(tmp_path)
    assert _setup(repo / "aws", "ENV=dev", f"REPO={origin}").returncode == 0
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=prod", f"REPO={other}")
    assert result.returncode != 0 and "nothing to change" not in result.stdout
    assert f"origin already points at {origin}, not {other}" in result.stderr
    assert _snapshot(repo) == before and not (repo / "aws/envs/prod").exists()


def test_same_repo_compares_user_port_and_host():
    same = sr.same_repo
    assert same("git@github.com:acme/gov.git", "ssh://git@github.com:22/acme/gov/")
    assert same("git@github.com:acme/gov.git", "https://github.com/acme/gov")
    assert same("https://github.com/acme/gov", "https://GITHUB.com:443/acme/gov.git")
    assert same("ssh://git@h.example:2222/a/b", "ssh://git@h.example:2222/a/b.git")
    assert not same("h.example:a/b", "ssh://h.example/a/b")  # unknown host: different forms
    assert not same("git@h.example:/abs/r", "git@h.example:abs/r")
    assert same("git@h.example:/abs/r", "git@H.example:/abs/r.git/")
    for hosted in ("github.com", "gitlab.com", "bitbucket.org"):
        assert same(f"git@{hosted}:g/sub/r.git", f"https://{hosted}/g/sub/r")
        assert same(f"ssh://git@{hosted}/g/sub/r", f"git@{hosted}:g/sub/r")
    assert not same("git@ssh.dev.azure.com:v3/org/proj/r", "https://dev.azure.com/org/proj/_git/r")
    assert same("https://dev.azure.com/org/proj/_git/r", "https://dev.azure.com/org/proj/_git/r/")
    assert same("file:///srv/gov.git", "/srv/gov/")
    assert not same("ssh://git@example.com:2222/acme/gov", "ssh://git@example.com/acme/gov")
    assert not same("ssh://alice@example.com/acme/gov", "ssh://bob@example.com/acme/gov")
    assert not same("ssh://alice@example.com/acme/gov", "ssh://example.com/acme/gov")
    assert not same("https://alice@github.com/acme/gov", "git@github.com:acme/gov")
    assert not same("h.example:a/b", "https://h.example/a/b")  # ssh without git@ vs https: unsure, refuse
    assert not same("https://github.com/acme/gov", "https://github.com/acme/Gov")
    assert not same("/srv/gov", "ssh://git@srv/gov")
    assert not same("not a url", "not a url")


def _worktree(tmp_path):
    """A tmp clone with one commit and a linked worktree of it; both share the clone's .git/config."""
    repo = _clone(tmp_path)
    for keep in ("aws/.keep", "shared/.keep"):
        (repo / keep).write_text("")
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "--no-verify", "-m", "init")
    wt = tmp_path / "wt"
    _git(repo, "worktree", "add", "-q", "--detach", str(wt))
    return repo, wt


def test_no_repo_in_a_linked_worktree_renames_nothing(tmp_path):
    repo, wt = _worktree(tmp_path)
    config = (repo / ".git/config").read_bytes()
    result = _setup(wt / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (repo / ".git/config").read_bytes() == config and _remotes(repo) == {"origin": UPSTREAM}
    assert "renamed" not in result.stdout
    assert f"linked worktree, its remotes shared through {(repo / '.git').resolve()}" in result.stdout


def test_repo_in_a_linked_worktree_is_refused_and_touches_nothing(tmp_path):
    repo, wt = _worktree(tmp_path)
    config = (repo / ".git/config").read_bytes()
    url = _customer_repo(tmp_path)
    result = _setup(wt / "aws", "ENV=dev", f"REPO={url}")
    assert result.returncode != 0
    assert f"is a linked git worktree: its remotes live in {(repo / '.git').resolve()}" in result.stderr
    assert (repo / ".git/config").read_bytes() == config and _remotes(repo) == {"origin": UPSTREAM}
    assert not (wt / "aws/envs").exists() and "=== Setup" not in result.stdout
    assert _no_refs(url)


def test_a_copied_worktree_gitfile_renames_nothing(tmp_path):
    """The 2026-10-28 incident: cp -R of a worktree keeps its .git file, and git init just reinitialises
    the shared repo, so the "copy" still writes the original clone's .git/config."""
    repo, wt = _worktree(tmp_path)
    copy = tmp_path / "copy"
    shutil.copytree(wt, copy, symlinks=True)
    _git(copy, "init", "-q", ".")
    config = (repo / ".git/config").read_bytes()
    result = _setup(copy / "aws", "ENV=bu2")
    assert result.returncode == 0, result.stdout + result.stderr
    assert (repo / ".git/config").read_bytes() == config and _remotes(repo) == {"origin": UPSTREAM}
    assert "renamed" not in result.stdout and "linked worktree" in result.stdout


@pytest.mark.parametrize("extra", ["fork", "upstream"])
def test_no_repo_leaves_a_contributor_clone_with_extra_remotes_alone(extra, tmp_path):
    repo = _clone(tmp_path)
    other = UPSTREAM if extra == "upstream" else "https://github.com/someone/genierails.git"
    _git(repo, "remote", "add", extra, other)
    before = _snapshot(repo)
    result = _setup(repo / "aws", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert _remotes(repo) == {"origin": UPSTREAM, extra: other} and "renamed" not in result.stdout
    assert f"origin isn't its only remote ({', '.join(sorted(['origin', extra]))})" in result.stdout
    assert _snapshot(repo)[1] == before[1]
