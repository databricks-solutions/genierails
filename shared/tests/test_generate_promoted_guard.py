"""make generate refuses a deterministic promoted env (S3), before anything is written.

A promoted env's generated/ comes only from promote-to: release-plan compares
it with what promote-to produces from the commit and refuses a regenerated one
as a stale promotion, so a generate there only costs a cycle. Legacy promoted
envs have no release-plan and still generate (e.g. a genie-only prod). A config
that doesn't parse keeps generate's own handling.
"""

from pathlib import Path

import pytest

from scripts import release_gate
from tests.test_check_target import _snapshot
from tests.test_deterministic_check_promote import (  # noqa: F401  (repo, promoted are fixtures)
    DEPLOYER, _auth, _envs, _make, promoted, repo,
)

pytestmark = pytest.mark.gnu_make


def _refusal(env, source):
    return (f"ERROR: make generate ENV={env} refused: ENV={env} is a deterministic env promoted from {source}, "
            f"so envs/{env}/generated/ comes only from promote-to")


def _assert_refused(result, env, source, envs, before):
    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert _refusal(env, source) in result.stderr, out
    assert "stale promotion" in result.stderr, out
    assert f"make generate ENV={source}" in result.stderr, out
    assert f"then make promote-to ENV={env}, then commit and push envs/{env}/" in result.stderr, out
    assert f"make release-plan ENV={env} and make release ENV={env}" in result.stderr, out
    # Refused before generate starts: no banner, no generate_abac run, nothing written.
    assert "=== Generate ABAC Config" not in out, out
    assert "Traceback" not in out, out
    assert _snapshot(envs) == before


def test_generate_refuses_a_deterministic_promoted_prod_and_writes_nothing(promoted, tmp_path):
    envs = _envs(promoted)
    before = _snapshot(envs)
    assert (envs / "prod" / "generated" / "abac.auto.tfvars").is_file()

    _assert_refused(_make(promoted, tmp_path, "generate", "ENV=prod"), "prod", "dev", envs, before)


def test_generate_refuses_prod_promoted_by_name_alone(promoted, tmp_path):
    """prod without promote_from is still promoted (from dev), as env-role says."""
    env_file = _envs(promoted) / "prod" / "env.auto.tfvars"
    env_file.write_text("\n".join(line for line in env_file.read_text().splitlines()
                                  if not line.startswith("promote_from")) + "\n")
    envs = _envs(promoted)
    before = _snapshot(envs)

    _assert_refused(_make(promoted, tmp_path, "generate", "ENV=prod"), "prod", "dev", envs, before)


def test_generate_refuses_a_deterministic_promoted_stg_and_writes_nothing(repo, tmp_path):
    stg = _envs(repo) / "stg"
    stg.mkdir()
    (stg / "env.auto.tfvars").write_text(
        f'governance_mode = "deterministic"\npromote_from = "dev"\ncatalog_map = {{ "paycat" = "spay" }}\n'
        f'deployer_principal_id = "{DEPLOYER}"\n')
    (stg / "auth.auto.tfvars").write_text(_auth(DEPLOYER))
    # A first run bootstraps the env layout; the refused run after it writes nothing.
    assert _make(repo, tmp_path, "generate", "ENV=stg").returncode != 0
    envs = _envs(repo)
    before = _snapshot(envs)

    _assert_refused(_make(repo, tmp_path, "generate", "ENV=stg"), "stg", "dev", envs, before)
    assert not any((stg / "generated").iterdir())


def test_generate_names_a_promote_from_other_than_dev(promoted, tmp_path):
    env_file = _envs(promoted) / "prod" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('promote_from = "dev"', 'promote_from = "stg"'))
    envs = _envs(promoted)
    before = _snapshot(envs)

    _assert_refused(_make(promoted, tmp_path, "generate", "ENV=prod"), "prod", "stg", envs, before)


def test_generate_starts_the_advice_where_the_promotion_chain_does(promoted, tmp_path):
    """prod from stg, stg from dev: stg can't generate either, so the advice starts in dev."""
    envs = _envs(promoted)
    (envs / "stg").mkdir()
    (envs / "stg" / "env.auto.tfvars").write_text('governance_mode = "deterministic"\npromote_from = "dev"\n')
    env_file = envs / "prod" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('promote_from = "dev"', 'promote_from = "stg"'))
    result = _make(promoted, tmp_path, "generate", "ENV=prod")
    assert result.returncode != 0, result.stdout + result.stderr
    assert ("Change the rules in dev: make generate ENV=dev, commit, make rehearse ENV=dev, then "
            "make promote-to ENV=stg, then make promote-to ENV=prod, then") in result.stderr, result.stderr
    assert "make generate ENV=stg" not in result.stderr


def test_promote_to_still_rewrites_the_promoted_env_after_a_refused_generate(promoted, tmp_path):
    """promote-to never runs the generate target (it runs promote from the source env)."""
    assert _make(promoted, tmp_path, "generate", "ENV=prod").returncode != 0
    result = _make(promoted, tmp_path, "promote-to", "ENV=prod")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refused" not in result.stdout + result.stderr
    check = _make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1")
    assert check.returncode == 0, check.stdout + check.stderr
    assert "OK: promote-to ENV=prod CHECK=1: committed files match." in check.stdout


def test_dev_generate_is_not_refused(promoted, tmp_path):
    """dev is a rehearsal env: generate runs as before (here it stops at the empty test secret)."""
    result = _make(promoted, tmp_path, "generate", "ENV=dev")
    out = result.stdout + result.stderr
    assert "=== Generate ABAC Config (dev) ===" in out, out
    assert "refused" not in out, out
    assert "fill in envs/dev/auth.auto.tfvars" in out, out


def test_legacy_promoted_prod_still_runs_generate(promoted, tmp_path):
    env_file = _envs(promoted) / "prod" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace('governance_mode = "deterministic"', 'governance_mode = "legacy"'))
    result = _make(promoted, tmp_path, "generate", "ENV=prod")
    out = result.stdout + result.stderr
    assert "=== Generate ABAC Config (prod) ===" in out, out
    assert "make generate ENV=prod refused" not in out, out


def test_malformed_promoted_config_keeps_generates_own_handling(promoted, tmp_path):
    env_file = _envs(promoted) / "prod" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + '\nbroken = "x" {\n')
    result = _make(promoted, tmp_path, "generate", "ENV=prod")
    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "make generate ENV=prod refused" not in out, out
    assert "generate-guard failed" not in out, out


# ── generate-guard on its own ──────────────────────────────────────────────

@pytest.mark.parametrize("name, text, refused", [
    ("prod", 'governance_mode = "deterministic"\n', True),
    ("stg", 'governance_mode = "deterministic"\npromote_from = "dev"\n', True),
    ("stg", 'governance_mode = "deterministic"\n', False),
    ("dev", 'governance_mode = "deterministic"\n', False),
    ("prod", 'governance_mode = "legacy"\npromote_from = "dev"\n', False),
    ("prod", "", False),
    ("prod", 'governance_mode = "deterministic"\nbroken = "x" {\n', False),
], ids=["det-prod", "det-promoted-stg", "det-rehearsal-stg", "det-dev", "legacy-prod", "no-config", "malformed"])
def test_generate_guard_cli(tmp_path, capsys, name, text, refused):
    env_dir = tmp_path / "envs" / name
    env_dir.mkdir(parents=True)
    if text:
        (env_dir / "env.auto.tfvars").write_text(text)
    rc = release_gate.main(["generate-guard", "--env-dir", str(env_dir), "--env", name])
    err = capsys.readouterr().err
    assert rc == (1 if refused else 0), err
    assert (f"make generate ENV={name} refused" in err) is refused
    if not refused:
        assert err == ""


def test_generate_guard_is_the_first_recipe_line():
    """The guard is the generate recipe's first line, ahead of auth checks and generate_abac."""
    recipe = (Path(__file__).resolve().parents[1] / "Makefile.shared").read_text().split("\ngenerate:", 1)[1]
    first = recipe.split("\n")[1]
    assert first.strip() == '@$(_RELEASE_GATE) generate-guard --env-dir "$(ENV_DIR)" --env "$(ENV)"'

