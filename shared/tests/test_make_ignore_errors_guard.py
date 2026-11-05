"""make -i/-k are refused at parse time unless the goal is read-only."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_check_target import DEV_ENV, _build, _env, _snapshot

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
MAKEFILE = SHARED / "Makefile.shared"
REFUSAL = "-i/-k are not supported for this target because guards would be skipped"
ALLOWLIST = {"help", "check", "status", "next", "validate"}
CONDITIONAL_ALLOW = {"evidence"}
SYNTAX_ERROR = "syntax error"
PROD_ENV = '''governance_mode = "deterministic"
promote_from = "dev"
catalog_map = { "paycat" = "ppay" }
deployer_principal_id = "d0000000-0000-0000-0000-00000000d001"
genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["ppay.s.t"] },
]
'''


def _gnu_make_version(binary: str) -> tuple[int, ...] | None:
    result = subprocess.run([binary, "--version"], text=True, capture_output=True, timeout=30)
    match = re.match(r"GNU Make (\d+)\.(\d+)", result.stdout)
    return tuple(map(int, match.groups())) if match else None


def _make_binaries() -> list[pytest.ParameterSet]:
    out = []
    apple = Path("/usr/bin/make")
    if apple.exists():
        out.append(pytest.param(str(apple), id="usr-bin-make"))
    gmake = shutil.which("gmake")
    if gmake and (_gnu_make_version(gmake) or (0,)) >= (4,):
        out.append(pytest.param(gmake, id="gmake-4"))
    return out


MAKE_BINARIES = _make_binaries()
if not MAKE_BINARIES:
    pytest.skip("make is required", allow_module_level=True)


def _tree_snapshot(root: Path) -> dict[str, bytes | str | None]:
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        if ".git" in dirnames:
            dirnames.remove(".git")
        for name in sorted(dirnames + filenames):
            path = Path(dirpath) / name
            out[str(path.relative_to(root))] = (
                os.readlink(path) if path.is_symlink() else path.read_bytes() if path.is_file() else None
            )
    return out


def _bare(tmp_path: Path) -> Path:
    repo = tmp_path / "bare"
    (repo / "aws").mkdir(parents=True)
    shutil.copy2(ROOT / "aws" / "Makefile", repo / "aws" / "Makefile")
    (repo / "shared").symlink_to(SHARED)
    return repo


def _promoted(tmp_path: Path) -> Path:
    repo = _build(tmp_path, DEV_ENV)
    prod = repo / "aws" / "envs" / "prod"
    prod.mkdir()
    (prod / "env.auto.tfvars").write_text(PROD_ENV)
    (prod / "generated").mkdir()
    shutil.copy2(repo / "aws" / "envs" / "dev" / "generated" / "abac.auto.tfvars",
                 prod / "generated" / "abac.auto.tfvars")
    return repo


def _make_env(tmp_path: Path, **extra: str) -> dict[str, str]:
    gnumakeflags = extra.pop("GNUMAKEFLAGS", None)
    env = _env(tmp_path, **extra)
    env.pop("GNUMAKEFLAGS", None)
    if gnumakeflags is not None:
        env["GNUMAKEFLAGS"] = gnumakeflags
    return env


def _make(make_bin: str, repo: Path, tmp_path: Path, *args: str,
          env: dict[str, str] | None = None, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        [make_bin, "--no-print-directory", "-C", str(repo / "aws"), *args],
        text=True,
        capture_output=True,
        timeout=timeout,
        env=_make_env(tmp_path, **(env or {})),
    )


def _assert_refused_without_change(make_bin: str, repo: Path, tmp_path: Path, *args: str,
                                   env: dict[str, str] | None = None) -> str:
    before = _tree_snapshot(repo)
    result = _make(make_bin, repo, tmp_path, *args, env=env)
    output = result.stdout + result.stderr
    assert SYNTAX_ERROR not in output
    assert result.returncode != 0, output
    assert REFUSAL in output, output
    assert _tree_snapshot(repo) == before
    return output


def _defined_targets() -> set[str]:
    targets = set()
    for raw in MAKEFILE.read_text().splitlines():
        if not raw or raw[0].isspace() or raw.startswith("."):
            continue
        left, sep, right = raw.partition(":")
        if not sep or "=" in left or left.endswith(" export"):
            continue
        if right.startswith("="):
            continue
        for name in left.split():
            if re.match(r"^[A-Za-z_][A-Za-z0-9_.-]*$", name):
                targets.add(name)
    return targets


REPRESENTATIVE_TARGETS = [
    ("generate", ("ENV=prod",), _promoted),
    ("setup", ("ENV=dev",), _bare),
    ("capture", ("ENV=prod", "SPACE=s1"), _promoted),
    ("scaffold-treatments", ("ENV=dev",), _promoted),
    ("materialize-treatment", ("ENV=dev", "TREATMENT=account_last4"), _promoted),
    ("derive-assignments", ("ENV=dev",), _promoted),
    ("suggest-treatment", ("ENV=dev", "CLASS=class.email"), _promoted),
    ("draft-genie-content", ("ENV=dev", "SPACE=s1"), _promoted),
    ("draft-custom-mask", ("ENV=dev", "TREATMENT=custom_x", "TYPE=STRING"), _promoted),
    ("adopt-custom-mask", ("ENV=dev", "TREATMENT=custom_x"), _promoted),
    ("plan", ("ENV=dev",), _promoted),
    ("wait-tag-policies", ("ENV=dev",), _promoted),
    ("verify-access", ("ENV=dev",), _promoted),
    ("promote", ("SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=paycat=ppay"), _promoted),
    ("rehearse", ("ENV=dev",), _promoted),
    ("release-plan", ("ENV=prod",), _promoted),
    ("apply", ("ENV=dev",), _promoted),
]

SHELL_SPECIAL_ASSIGNMENTS = [
    pytest.param("NOTE=it's", id="single-quote"),
    pytest.param('NOTE=a"b', id="double-quote"),
    pytest.param("NOTE=a;b", id="semicolon"),
    pytest.param("NOTE=x&y", id="ampersand"),
    pytest.param("NOTE=x|y", id="pipe"),
    pytest.param("NOTE=x<y", id="redirect"),
    pytest.param("NOTE=x)y", id="close-paren"),
    pytest.param("NOTE=$(printf bad)", id="make-reference"),
]


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize(("target", "target_args", "builder"), REPRESENTATIVE_TARGETS)
@pytest.mark.parametrize(("mode_args", "env"), [
    pytest.param(("-i", "-j8"), {}, id="make-i-j8"),
    pytest.param(("-ik", "-j8"), {}, id="make-ik-j8"),
    pytest.param(("-k", "-j8"), {}, id="make-k-j8"),
    pytest.param(("--ignore-errors",), {}, id="long-ignore-errors"),
    pytest.param(("--keep-going",), {}, id="long-keep-going"),
    pytest.param((), {"MAKEFLAGS": "-i"}, id="makeflags-i"),
    pytest.param((), {"MAKEFLAGS": "-k"}, id="makeflags-k"),
])
def test_state_changing_targets_refuse_error_modes_before_any_write(make_bin, tmp_path, target, target_args,
                                                                    builder, mode_args, env):
    repo = builder(tmp_path)
    _assert_refused_without_change(make_bin, repo, tmp_path, *mode_args, target, *target_args, env=env)


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize("assignment", SHELL_SPECIAL_ASSIGNMENTS)
def test_error_mode_parser_ignores_shell_special_command_line_assignments(make_bin, tmp_path, assignment):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(make_bin, repo, tmp_path, "-i", "capture", "ENV=prod", "SPACE=s1", assignment)


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize("assignment", SHELL_SPECIAL_ASSIGNMENTS)
def test_error_mode_parser_refuses_apply_with_shell_special_assignments(make_bin, tmp_path, assignment):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(make_bin, repo, tmp_path, "-i", "apply", "ENV=dev", assignment)


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize("assignment", SHELL_SPECIAL_ASSIGNMENTS)
def test_keep_going_refuses_before_parallel_writes_with_shell_special_assignments(make_bin, tmp_path, assignment):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(
        make_bin, repo, tmp_path, "-k", "-j8", "apply", "ENV=prod", assignment, env={"CI": "true"}
    )


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize("assignment", SHELL_SPECIAL_ASSIGNMENTS)
def test_read_only_help_allows_shell_special_assignments_without_syntax_error(make_bin, tmp_path, assignment):
    repo = _bare(tmp_path)
    result = _make(make_bin, repo, tmp_path, "-n", "help", assignment)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert REFUSAL not in output
    assert SYNTAX_ERROR not in output


@pytest.mark.skipif(not shutil.which("gmake"), reason="needs GNU Make 4.x as gmake")
@pytest.mark.parametrize("env", [{"GNUMAKEFLAGS": "-i"}, {"GNUMAKEFLAGS": "-k"}])
def test_gnumakeflags_are_checked_for_gnu_make_4(tmp_path, env):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(shutil.which("gmake"), repo, tmp_path, "apply", "ENV=dev", env=env)


@pytest.mark.skipif(not shutil.which("gmake"), reason="needs GNU Make 4.x as gmake")
def test_gnu_makeflags_real_error_flag_before_eval_still_refuses(tmp_path):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(
        shutil.which("gmake"), repo, tmp_path, "-i", "--eval", "NOTE := harmless", "apply", "ENV=dev"
    )


@pytest.mark.skipif(not shutil.which("gmake"), reason="needs GNU Make 4.x as gmake")
def test_gnu_makeflags_eval_text_does_not_false_refuse(tmp_path):
    repo = _promoted(tmp_path)
    result = _make(
        shutil.which("gmake"), repo, tmp_path, "--eval", "NOTE := text-with--ignore-errors-and-k", "-n",
        "apply", "ENV=dev",
    )
    assert REFUSAL not in result.stdout + result.stderr


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
def test_no_goal_uses_default_help_and_is_allowed_under_error_modes(make_bin, tmp_path):
    repo = _bare(tmp_path)
    result = _make(make_bin, repo, tmp_path, "-i", "-n")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert REFUSAL not in output


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
def test_every_makefile_target_is_refused_by_default_or_explicitly_allowed(make_bin, tmp_path):
    repo = _bare(tmp_path)
    for target in sorted(_defined_targets()):
        args = ["-i", "-n", target, "ENV=dev"]
        if target == "evidence":
            args.append("OFFLINE=1")
        result = _make(make_bin, repo, tmp_path, *args)
        output = result.stdout + result.stderr
        if target in ALLOWLIST or target in CONDITIONAL_ALLOW:
            assert REFUSAL not in output, f"{target} should be allowed under -i:\n{output}"
        else:
            assert result.returncode != 0, f"{target} unexpectedly ran under -i:\n{output}"
            assert REFUSAL in output, f"{target} failed for the wrong reason:\n{output}"


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
def test_evidence_is_allowed_only_when_offline(make_bin, tmp_path):
    repo = _bare(tmp_path)
    refused = _make(make_bin, repo, tmp_path, "-i", "-n", "evidence", "ENV=dev")
    assert REFUSAL in refused.stdout + refused.stderr
    allowed = _make(make_bin, repo, tmp_path, "-i", "-n", "evidence", "ENV=dev", "OFFLINE=1")
    output = allowed.stdout + allowed.stderr
    assert REFUSAL not in output


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize("target", [
    ("help",),
    ("validate", "ENV=dev"),
    ("check", "ENV=dev"),
    ("status", "ENV=dev"),
    ("next", "ENV=dev"),
    ("evidence", "ENV=dev", "OFFLINE=1"),
])
def test_read_only_targets_still_run_under_ignore_errors(make_bin, tmp_path, target):
    repo = _promoted(tmp_path)
    before = _snapshot(repo / "aws" / "envs")
    result = _make(make_bin, repo, tmp_path, "-i", *target)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert REFUSAL not in output
    assert _snapshot(repo / "aws" / "envs") == before


@pytest.mark.skipif(not shutil.which("gmake"), reason="needs GNU Make 4.x as gmake")
@pytest.mark.parametrize("args", [
    ("-I", "/opt/homebrew/include"),
    ("--output-sync=line",),
    ("--include-dir=/tmp/inc",),
])
def test_gnu_make_4_long_and_arg_options_do_not_false_match_i(tmp_path, args):
    repo = _promoted(tmp_path)
    result = _make(shutil.which("gmake"), repo, tmp_path, "-n", *args, "apply", "ENV=dev")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert REFUSAL not in output


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
def test_ci_keep_going_apply_refuses_before_parallel_prerequisites_write(make_bin, tmp_path):
    repo = _promoted(tmp_path)
    _assert_refused_without_change(make_bin, repo, tmp_path, "-k", "-j8", "apply", "ENV=prod", env={"CI": "true"})


@pytest.mark.parametrize("make_bin", MAKE_BINARIES)
@pytest.mark.parametrize(("args", "env"), [
    pytest.param(("GENIERAILS_READONLY_ERROR_FLAGS=1",), {}, id="command-line"),
    pytest.param((), {"GENIERAILS_READONLY_ERROR_FLAGS": "1"}, id="environment"),
])
def test_internal_readonly_error_flag_cannot_be_forged_at_top_level(make_bin, tmp_path, args, env):
    repo = _bare(tmp_path)
    result = _make(make_bin, repo, tmp_path, "-i", "apply", "ENV=dev", *args, env=env)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "GENIERAILS_READONLY_ERROR_FLAGS is internal" in output
