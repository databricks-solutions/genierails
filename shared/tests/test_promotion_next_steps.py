"""What comes after promote-to, everywhere it is printed or documented.

release-plan plans only a commit on origin/main that has a passing rehearsal of
the destination's source env (its promote_from, dev by default) at that same
commit. So a deterministic promotion is always: promote-to, commit and push (or
merge the PR), rehearse the source env on that commit, release-plan, release.
Legacy envs have no release-plan and release directly.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).parents[1]
ROOT = SHARED.parent
sys.path.insert(0, str(SHARED))
sys.path.insert(0, str(SHARED / "scripts"))

from scripts import coverage_fix, migration  # noqa: E402
import classification_completeness  # noqa: E402
import run_scheduled_governance  # noqa: E402
from generate_abac import generate_next_steps  # noqa: E402

DETERMINISTIC_STG = 'governance_mode = "deterministic"\npromote_from = "stg"\n'
DETERMINISTIC = 'governance_mode = "deterministic"\n'
LEGACY = 'governance_mode = "legacy"\n'
FULL_STG = ["commit and push envs/prod/", "make rehearse ENV=stg on that commit", "make release-plan ENV=prod",
            "make release ENV=prod"]


@pytest.mark.parametrize("env,promoted,expected,absent", [
    ("stg", True, "make release-plan ENV=stg", "make rehearse ENV=stg   (applies"),
    ("stg", False, "make rehearse ENV=stg   (applies", "make release-plan ENV=stg"),
    ("prod", True, "make release-plan ENV=prod", "make rehearse ENV=prod   (applies"),
    ("dev", False, "make rehearse ENV=dev   (applies", "make release-plan ENV=dev"),
])
def test_generate_banner_follows_env_role(env, promoted, expected, absent, tmp_path):
    banner = "\n".join(generate_next_steps(tmp_path, "full", env, has_sql=True,
                                             champion_flow=True, tuning=False,
                                             source_env="dev", promoted=promoted))
    assert expected in banner
    assert absent not in banner


@pytest.mark.parametrize("env", ["prod", "stg"])
def test_generate_banner_keeps_legacy_promoted_envs_on_release(env, tmp_path):
    banner = "\n".join(generate_next_steps(tmp_path, "full", env, has_sql=True,
                                             champion_flow=True, tuning=True,
                                             source_env="dev", promoted=True))
    assert f"make release ENV={env}" in banner
    assert f"make rehearse ENV={env}" not in banner
    assert "make release-plan" not in banner


def _text(path: str) -> str:
    return " ".join((ROOT / path).read_text().split())


def _in_order(text: str, steps: list[str]) -> bool:
    pos = 0
    for step in steps:
        found = text.find(step, pos)
        if found < 0:
            return False
        pos = found + len(step)
    return True


SRC = "rehearse the source env (`promote_from`, dev by default)"
PLAN = ["`release-plan`", "`release`"]
MAKE_PLAN = ["make release-plan ENV=prod", "make release ENV=prod"]

# file -> where the passage starts -> what must follow, in order.
DOC_ORDER = [
    ("shared/examples/dev_to_prod/README.md", "make promote-to ENV=prod # copies the reviewed rules",
     ["git push", "make rehearse ENV=dev", *MAKE_PLAN]),
    ("shared/examples/dev_to_prod/README.md", "**3. Check, rehearse, plan, release.**",
     ["make check ENV=prod", "make rehearse ENV=dev", *MAKE_PLAN]),
    ("shared/examples/dev_to_prod/README.md", "plans only a commit on your `origin/main`",
     ["make rehearse ENV=dev", "dev is prod's `promote_from` in this walkthrough", "make rehearse ENV=stg"]),
    ("shared/examples/dev_to_prod/README.md", "**Reuse an existing mask** (the common case)",
     ["`make promote-to ENV=prod`", "`make rehearse ENV=dev` (again", "`make release-plan ENV=prod`",
      "`make release ENV=prod`"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "| `make release-plan ENV=prod` | 4 |",
     ["`make rehearse ENV=dev` receipt", "dev is prod's `promote_from` here", "`make rehearse ENV=stg`"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "Stops at `coverage-gate` or `audit-rulebook`",
     ["`promote-to`", "push to `origin/main`", "`make rehearse ENV=dev` on that commit",
      "`make release-plan ENV=prod`", "`make release ENV=prod`"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "Deterministic envs (`governance_mode = \"deterministic\"`) never",
     ["`make generate ENV=dev`", "`make check ENV=dev`", "`make promote-to ENV=prod`", "`make check ENV=prod`",
      "commit and push", "`make rehearse ENV=dev` again", "`make release-plan ENV=prod`", "`make release ENV=prod`"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "A new kind of mask is a custom mask:",
     ["`make adopt-custom-mask ENV=dev TREATMENT=custom_<name>`", "`tag_treatments`", "then the same steps"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "Later legacy `make generate ENV=dev` runs keep the mask",
     ["It refuses a deterministic env and writes nothing", "map the class or adopt a custom mask"]),
    ("shared/examples/dev_to_prod/REFERENCE.md", "- Then `make rehearse ENV=dev`, `make promote-to ENV=prod`",
     ["`make promote-to ENV=prod`", "`make release ENV=prod`", "legacy envs have no `release-plan`"]),
    ("REFERENCE.md", "| `make release-plan` |",
     ["a passing clean rehearsal of HEAD in the env's source env, its `promote_from` (default `dev`)"]),
    ("README.md", "promote the *rules* to prod", ["release a rehearsed commit", *PLAN]),
    ("shared/docs/pipelines.md", "**`make release-plan ENV=prod`** refuses",
     ["of prod's source env (`promote_from`, dev by default)"]),
    ("shared/docs/access.md", "`make promote-to ENV=prod` (seeds prod's",
     ["push that to `origin/main`", "`make rehearse` again on that commit", "`make release-plan ENV=prod`",
      "`make release ENV=prod`"]),
    ("shared/docs/import-genie-agent-from-ui.md", "The remaining workflow is unchanged",
     ["promote", "merge the promotion", SRC, *PLAN]),
    ("shared/docs/legacy-migration.md", "make promote-to ENV=prod # 2d.",
     ["make rehearse ENV=dev", *MAKE_PLAN]),
    ("shared/docs/design/deterministic-governance.md", "6. Approve and merge the promotion PR",
     ["once rehearse passes on that commit", "`make release-plan ENV=prod`", "`make release ENV=prod`"]),
    *[("shared/docs/design/deterministic-governance.md", row,
       ["merge", "rehearse the source env on", *PLAN])
      for row in ("| Promote A while B has unfinished dev edits |", "| Mask change while B is mid-curation |",
                  "| Grant another group access to A in prod |", "| Change who can read a table |",
                  "| Upgrade GenieRails |")],
    ("shared/docs/design/deterministic-governance.md", "| Stop governing a table |",
     ["a PR applies the removal: merge", "rehearse on the merge commit", *PLAN]),
    ("shared/docs/playbooks/agents.md", "make promote-to ENV=prod # carries the capture",
     ["rehearse dev on the merge commit", *MAKE_PLAN]),
    ("shared/docs/playbooks/agents.md", "3. **Merge.** After the rehearsal of",
     ["the source env (`promote_from`, dev by default) passes on the merge commit",
      "`make release-plan ENV=prod`", "`make release ENV=prod`"]),
    ("shared/docs/playbooks/agents.md", "governance adds it to `table_readers` in the same PR",
     ["merge", "rehearse the source env (`promote_from`, dev by default) on the merged commit", *PLAN]),
    ("shared/docs/playbooks/agents.md", "# merge, rehearse dev on the merge commit",
     ["release-plan", "release"]),
    ("shared/docs/playbooks/governance.md", "make promote-to ENV=prod # the promotion PR",
     ["rehearse dev on the merge commit", *MAKE_PLAN]),
    ("shared/docs/playbooks/governance.md", "make promote-to ENV=prod # commit envs/prod/, PR, merge",
     ["make rehearse ENV=dev # again, on the promotion's merge commit", *MAKE_PLAN]),
    ("shared/docs/playbooks/governance.md", "`catalog_map = { \"dev_finance\" = \"prod_finance\" }`, promote",
     ["merge the promotion", SRC, *PLAN]),
    ("shared/docs/playbooks/governance.md", "promoted from. Then generate",
     ["`promote-to`", "merge to `origin/main`", SRC, *PLAN]),
    ("shared/docs/playbooks/governance.md", "`make promote-to ENV=prod` in the same PR. Then merge",
     [f"{SRC} on the merged commit", *PLAN]),
    ("shared/docs/playbooks/one-team.md", "make promote-to ENV=prod # same PR",
     ["after rehearse passes on the merge commit", *MAKE_PLAN]),
    ("shared/docs/playbooks/one-team.md", "5. **Merge.**",
     ["`make rehearse ENV=dev` on the merge commit", "6. **Release.**", "`make release-plan ENV=prod`",
      "`make release ENV=prod`"]),
    *[("shared/docs/playbooks/one-team.md", row, ["merge, rehearse dev on the merge commit", *PLAN])
      for row in ("| Give another group an agent in prod |", "| Remove an agent |",
                  "| Remove a table from an agent |")],
]


@pytest.mark.parametrize("path, start, steps", DOC_ORDER, ids=[f"{p}::{s[:40]}" for p, s, _ in DOC_ORDER])
def test_docs_rehearse_the_promotion_commit_before_release_plan(path, start, steps):
    text = _text(path)
    assert start in text, f"{path}: passage {start!r} is gone"
    passage = text[text.index(start):][:700]
    assert _in_order(passage, steps), f"{path}: {passage[:300]}"


# A list-style "promote, release" / "PR, release" with no release-plan between.
SHORTCUT = re.compile(r"(?:\bpromot[\w-]*`?(?:\s+ENV=\S+?)?`?|\bPR|\bmerge)\s*(?:,|;|→|->)\s*(?:and\s+|then\s+)?"
                      r"(?:re-run\s+|run\s+)?`?(?:make\s+)?release\b(?!-plan)", re.IGNORECASE)
# Not deterministic flows: the legacy one-step release and a label of what a test scenario runs.
ALLOWED = ("legacy envs have no `release-plan`", "Scenario: dev to prod champion flow")


def test_no_doc_goes_from_promote_or_a_pr_straight_to_release():
    tracked = subprocess.run(["git", "ls-files", "*.md"], cwd=ROOT, text=True, capture_output=True, check=True)
    found = []
    for path in tracked.stdout.split():
        if "CHANGELOG" in path:
            continue
        text = _text(path)
        for match in SHORTCUT.finditer(text):
            context = text[max(0, match.start() - 120):match.end() + 40]
            if not any(allowed in context for allowed in ALLOWED):
                found.append(f"{path}: …{context}…")
    assert not found, "\n".join(found)


# ── Printed next steps ───────────────────────────────────────────────────────


def _env(tmp_path: Path, name: str, text: str) -> Path:
    env_dir = tmp_path / "envs" / name
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(text)
    return env_dir


def _clean_env():
    drop = {"MAKEFLAGS", "MAKELEVEL", "ENV", "CI", "GENIERAILS_ALLOW_CI_APPLY"}
    return {key: value for key, value in os.environ.items() if key not in drop}


def _make(tmp_path: Path, *args: str, recipe: str = "") -> subprocess.CompletedProcess:
    extra = ["--eval", f"_t: ; @{recipe}", "_t"] if recipe else []
    return subprocess.run(
        ["make", "--no-print-directory", *extra, *args, f"CLOUD_ROOT={tmp_path}", f"SHARED_ROOT={SHARED}"],
        cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env())


@pytest.mark.parametrize("target", ["release", "release-plan", "maintain"])
def test_the_drift_hint_prints_the_full_sequence_in_a_deterministic_env(tmp_path, target):
    _env(tmp_path, "prod", DETERMINISTIC_STG)
    result = _make(tmp_path, "ENV=prod", recipe=f"$(_DRIFT_HINT) --target {target}")
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert _in_order(out, ["fix the rule in stg", "make rehearse ENV=stg", "make promote-to ENV=prod", *FULL_STG]), out
    assert f"re-run make {target}" not in out


@pytest.mark.parametrize("target", ["release", "maintain"])
def test_the_drift_hint_keeps_the_direct_rerun_in_a_legacy_env(tmp_path, target):
    _env(tmp_path, "prod", LEGACY)
    result = _make(tmp_path, "ENV=prod", recipe=f"$(_DRIFT_HINT) --target {target}")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == (
        "For class.* tags, follow 'How to fix' above; otherwise fix the rule in dev, make rehearse, "
        f"make promote-to ENV=prod, then re-run make {target} ENV=prod.")
    assert "release-plan" not in result.stdout


def test_every_drift_branch_prints_the_hint_for_its_own_target():
    makefile = (SHARED / "Makefile.shared").read_text()
    for target in ("release", "release-plan", "maintain"):
        assert f"$(_DRIFT_HINT) --target {target} >&2" in makefile
    assert "fix the rule in dev, make rehearse, make promote-to" not in makefile


@pytest.mark.parametrize("text, expected", [
    (DETERMINISTIC_STG, "commit and push envs/prod/ to origin/main (or merge its PR), make rehearse ENV=stg on that "
                        "commit, make release-plan ENV=prod and make release ENV=prod"),
    (DETERMINISTIC, "commit and push envs/prod/ to origin/main (or merge its PR), make rehearse ENV=dev on that "
                    "commit, make release-plan ENV=prod and make release ENV=prod"),
    (LEGACY, "make release ENV=prod"),
])
def test_release_steps_follow_the_env_mode_and_its_promote_from(tmp_path, text, expected):
    prod = _env(tmp_path, "prod", text)
    result = _make(tmp_path, "ENV=prod", recipe='$(_RELEASE_STEPS) --env-dir "$(ENV_DIR)" --env prod')
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected == coverage_fix.release_steps("prod", prod)


def test_promote_and_ungovern_print_the_mode_aware_release_steps():
    makefile = (SHARED / "Makefile.shared").read_text()
    promote = makefile[makefile.index("=== Promote complete:"):]
    promote = promote[:promote.index("trap - EXIT")]
    assert '$(_RELEASE_STEPS) --env-dir "$$dest_env_dir" --env "$(DEST_ENV)" --source "$(SOURCE_ENV)"' in promote
    ungovern = makefile[makefile.index("ENV=$(ENV) is promoted, so nothing was applied."):][:300]
    assert '$(_RELEASE_STEPS) --env-dir "$(ENV_DIR)" --env "$(ENV)"' in ungovern
    assert "after rehearse, make release-plan" not in makefile


@pytest.mark.parametrize("auth_text, expected, absent", [
    (None, "Next: edit", "not valid HCL"),
    ('databricks_client_secret = "unterminated\n', "Next: fix", "Next: edit"),
])
def test_promote_auth_hint_distinguishes_missing_from_malformed(tmp_path, auth_text, expected, absent):
    makefile = (SHARED / "Makefile.shared").read_text()
    promote = makefile[makefile.index("=== Promote complete:"):]
    assert 'if [ $$status -eq 2 ]; then' in promote
    assert 'Next: fix $$dest_env_dir/auth.auto.tfvars (not valid HCL)' in promote
    assert 'elif [ $$status -ne 0 ]; then' in promote
    assert 'Next: edit $$dest_env_dir/auth.auto.tfvars' in promote
    auth = tmp_path / "auth.auto.tfvars"
    if auth_text is not None:
        auth.write_text(auth_text)
    recipe = (
        'status=0; python3 "$(SHARED_ROOT)/scripts/auth_configured.py" "$(CLOUD_ROOT)/auth.auto.tfvars" '
        '|| status=$$?; if [ $$status -eq 2 ]; then echo "Next: fix (not valid HCL)"; '
        'elif [ $$status -ne 0 ]; then echo "Next: edit (workspace credentials)"; fi'
    )
    result = _make(tmp_path, recipe=recipe)
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout
    assert absent not in result.stdout


@pytest.mark.parametrize("text, expected", [
    (DETERMINISTIC_STG, "rehearse: ENV=prod is not allowed; rehearse its source env, then make release-plan ENV=prod "
                        "and make release ENV=prod"),
    (LEGACY, "rehearse: ENV=prod is not allowed; use make release ENV=prod"),
])
def test_rehearse_in_prod_points_at_the_release_for_the_env_mode(tmp_path, text, expected):
    _env(tmp_path, "prod", text)
    result = _make(tmp_path, "rehearse", "ENV=prod")
    assert result.returncode != 0
    assert expected in result.stderr


def test_a_failed_deterministic_release_never_suggests_make_apply(tmp_path):
    prod = _env(tmp_path, "prod", DETERMINISTIC_STG)
    result = subprocess.run([sys.executable, str(SHARED / "scripts" / "release_helpers.py"), "failed", str(prod),
                             "--env", "prod", "--reason", "apply failed"], text=True, capture_output=True)
    text = " ".join(result.stderr.split())
    assert "make apply" not in text
    assert _in_order(text, ["in a PR, then after merge:", "make rehearse ENV=stg on the merge commit", *MAKE_PLAN])
    assert _in_order(text, ["Otherwise fix the cause", *MAKE_PLAN])


def test_a_failed_legacy_release_keeps_apply_then_rerun(tmp_path):
    prod = _env(tmp_path, "prod", LEGACY)
    result = subprocess.run([sys.executable, str(SHARED / "scripts" / "release_helpers.py"), "failed", str(prod),
                             "--env", "prod", "--reason", "apply failed"], text=True, capture_output=True)
    assert "then run: make apply ENV=prod" in result.stderr
    assert "Then fix the cause and re-run make release ENV=prod." in result.stderr


@pytest.mark.parametrize("text, steps", [
    (DETERMINISTIC_STG, FULL_STG),
    (LEGACY, ["make release ENV=prod"]),
])
def test_scheduled_governance_and_classification_follow_the_env_mode(tmp_path, text, steps):
    prod = _env(tmp_path, "prod", text)
    assert _in_order(run_scheduled_governance._release_steps("prod", prod), steps)
    assert run_scheduled_governance._source_env(prod) == ("stg" if "stg" in text else "dev")
    assert _in_order(classification_completeness._release_steps(prod), steps)


@pytest.mark.parametrize("name, text, expected", [
    ("prod", DETERMINISTIC_STG, ["merge", "make rehearse ENV=stg on the merge commit", *MAKE_PLAN]),
    ("prod", DETERMINISTIC, ["merge", "make rehearse ENV=dev on the merge commit", *MAKE_PLAN]),
    ("dev", DETERMINISTIC, ["merge, then make rehearse ENV=dev"]),
])
def test_a_ready_cutover_rehearses_the_source_before_release_plan(tmp_path, name, text, expected):
    steps = migration.cutover_next(_env(tmp_path, name, text), name)
    assert _in_order(steps, expected), steps
    if name == "dev":
        assert "release-plan" not in steps


PLACEHOLDER_PROD = ('governance_mode = "deterministic"\n'
                    'deployer_principal_id = "<your-deployer-principal-id>"\n'
                    'access_tier_groups = ["<most-privileged-group>", "<least-privileged-group>"]\n'
                    'genie_spaces = [{ name = "Inline", genie_space_id = "<inline-id>", acl_groups = ["<inline-group>"] }]\n')
PLACEHOLDER_AGENT = 'genie_space_id = "<your-genie-space-id>"\nacl_groups = ["<group-that-may-run-the-agent>"]\n'


def test_placeholder_errors_lists_every_problem_in_fix_order(tmp_path):
    from genie_space_placeholder import agent_files_placeholder_errors, placeholder_error, placeholder_errors
    import hcl2

    env_dir = _env(tmp_path, "prod", PLACEHOLDER_PROD)
    (env_dir / "agents").mkdir()
    (env_dir / "agents/Finance.auto.tfvars").write_text(PLACEHOLDER_AGENT)
    config = hcl2.loads(PLACEHOLDER_PROD)

    errors = placeholder_errors(config, env_dir / "env.auto.tfvars")
    assert [e.split(" in ")[0] for e in errors] == [
        "replace placeholder <inline-id>", "replace placeholder <inline-group>",
        "replace placeholder <your-deployer-principal-id>", "replace placeholder <most-privileged-group>"]
    assert placeholder_error(config, env_dir / "env.auto.tfvars") == errors[0]
    # A promoted env's tiers come from promotion; everything else is still reported.
    assert placeholder_errors(config, env_dir / "env.auto.tfvars", allow_access_tier_placeholders=True) == errors[:3]
    agent_errors = agent_files_placeholder_errors(env_dir)
    assert len(agent_errors) == 2
    assert "<your-genie-space-id> in envs/prod/agents/Finance.auto.tfvars" in agent_errors[0]
    assert "<group-that-may-run-the-agent> in envs/prod/agents/Finance.auto.tfvars" in agent_errors[1]


def test_promote_to_reports_every_unfilled_placeholder_at_once(tmp_path):
    """promote-to refuses until the deployer and agent IDs are filled: one run names them all (finding #21)."""
    env_dir = _env(tmp_path, "prod", PLACEHOLDER_PROD)
    (env_dir / "agents").mkdir()
    (env_dir / "agents/Finance.auto.tfvars").write_text(PLACEHOLDER_AGENT)

    result = _make(tmp_path, "promote-to", "ENV=prod")

    assert result.returncode != 0, result.stdout + result.stderr
    errors = [line for line in result.stderr.splitlines() if line.startswith("ERROR: replace placeholder")]
    placeholders = [re.match(r"ERROR: replace placeholder (<[^>]+>)", line).group(1) for line in errors]
    assert placeholders == ["<your-genie-space-id>", "<group-that-may-run-the-agent>", "<inline-id>",
                            "<inline-group>", "<your-deployer-principal-id>"], result.stderr
    # The guard runs before promotion writes anything.
    assert not (env_dir / "generated").exists()


def test_prod_setup_banner_asks_for_the_deployer_before_promote_to():
    makefile = (SHARED / "Makefile.shared").read_text()
    banner = makefile[makefile.index("Next steps ($(ENV) promotion"):makefile.index("  4. In Catalog Explorer")]
    assert (banner.index("Create or confirm the $(ENV) deployment SP")
            < banner.index("deployer_note")
            < banner.index("promote-to refuses while deployer_principal_id is still a placeholder")
            < banner.index("make promote-to ENV=$(ENV)")), banner


def test_prod_setup_says_set_group_map_not_replace_access_tier_groups():
    """promote-to writes prod's tiers from dev's through group_map, so prod's own tier names
    without a group_map are refused; README Prod step 1 and the setup banner say so."""
    readme = (ROOT / "shared/examples/dev_to_prod/README.md").read_text()
    step = readme[readme.index("**1. Set up and promote.**"):readme.index("**2. Classify")]
    assert "group_map" in step and "Leave the `access_tier_groups` placeholders" in step, step
    assert "`access_tier_groups` placeholders, and set" not in step
    makefile = (SHARED / "Makefile.shared").read_text()
    banner = makefile[makefile.index("Next steps ($(ENV) promotion"):makefile.index("  2. In $(_SETUP_ENV_REL)/env.auto.tfvars")]
    assert "set group_map there" in banner and "leave those placeholders" in banner, banner
    assert "Replace this env's access_tier_groups placeholders" not in banner
    # promote-to (and the promote it wraps) accept the destination's tier placeholders it overwrites.
    for guard in ("override _PROMOTE_TO_PLACEHOLDER_GUARD", "override _GENIE_PLACEHOLDER_GUARD"):
        line = makefile[makefile.index(guard):].split("\n\n")[0]
        assert "GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS=1" in line, line


def test_prod_setup_advice_does_not_ask_to_replace_tier_placeholders(tmp_path):
    env = tmp_path / "env.auto.tfvars"
    env.write_text('governance_mode = "deterministic"\n'
                   'access_tier_groups = ["<most-privileged-group>", "<least-privileged-group>"]\n')
    script = SHARED / "genie_space_placeholder.py"
    makefile = (SHARED / "Makefile.shared").read_text()
    # A promoted env (prod, or any env with promote_from) gets its tiers from promotion.
    advice_line = makefile[makefile.index("\t@allow=0;"):makefile.index("--advice")]
    assert 'env-role' in advice_line and '= promoted ] && allow=1' in advice_line, advice_line
    assert "GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS=$$allow python3" in advice_line, advice_line
    advice = {
        flag: subprocess.run([sys.executable, str(script), "--advice", str(env)], text=True, capture_output=True,
                             env={**os.environ, "GENIERAILS_ALLOW_ACCESS_TIER_PLACEHOLDERS": flag}).stdout
        for flag in ("0", "1")
    }
    assert "access_tier_groups placeholder" in advice["0"]
    assert advice["1"] == ""


# ── A coverage gap's recovery sequence: one builder, the README's list ──────

README = SHARED / "examples" / "dev_to_prod" / "README.md"
DETERMINISTIC_PROD = 'governance_mode = "deterministic"\npromote_from = "dev"\n'
MAKE_STEP = r"make [a-z-]+ ENV=[a-z]+"


def _readme_recovery() -> list[str]:
    """The make commands the README lists after a reused mask's commit, in order."""
    line = next(line for line in README.read_text().splitlines() if line.startswith("- **Reuse an existing mask**"))
    tail = line[line.index("Commit it, then"):]
    assert "committing and pushing after each step that changes `envs/`" in tail
    return re.findall(rf"`({MAKE_STEP})`", tail)


def _gap_envs(tmp_path: Path) -> tuple[Path, Path]:
    for name, text in (("dev", DETERMINISTIC), ("prod", DETERMINISTIC_PROD)):
        (tmp_path / "envs" / name).mkdir(parents=True)
        (tmp_path / "envs" / name / "env.auto.tfvars").write_text(text)
    return tmp_path / "envs" / "dev", tmp_path / "envs" / "prod"


def _scaffold_output(capsys, env_name: str, env_dir: Path, target: Path) -> str:
    from scripts import scaffold_treatments

    scaffold_treatments._print_deterministic({
        "already": [], "labels": ["class.loyalty_id"], "treatment": "redact", "columns": [],
        "types": ["STRING"], "target": target, "env_dir": env_dir}, env_name)
    return capsys.readouterr().out


def test_every_coverage_gap_fix_prints_the_readme_recovery_sequence(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("GENIERAILS_RERUN_TARGET", raising=False)
    dev, prod = _gap_envs(tmp_path)
    readme = _readme_recovery()
    assert readme == ["make generate ENV=dev", "make check ENV=dev", "make rehearse ENV=dev",
                      "make promote-to ENV=prod", "make check ENV=prod", "make rehearse ENV=dev",
                      "make release-plan ENV=prod", "make release ENV=prod"]
    builder = coverage_fix.recovery_steps(["dev", "prod"], "envs/dev/env.auto.tfvars")
    assert builder.startswith("commit envs/dev/env.auto.tfvars, make generate ENV=dev")
    assert builder.endswith("; commit and push after each step that changes envs/")
    assert re.findall(MAKE_STEP, builder) == readme

    scaffold = _scaffold_output(capsys, "prod", prod, dev)
    assert f"Next: {builder}\n" in scaffold
    fix = "\n".join(coverage_fix.fix_lines("prod", prod, [("ppay.s.t.loyalty", "class.loyalty_id")]))
    assert f"then {builder}" in fix
    assert re.findall(MAKE_STEP, fix.split("then ", 1)[1]) == readme
    # A missing mask is fixed by mapping too: a deterministic generate replaces a materialized one.
    mask = "\n".join(coverage_fix.materialize_lines("prod", prod, ["generic_partial"]))
    assert "make materialize-treatment" not in mask
    assert "make adopt-custom-mask ENV=dev TREATMENT=custom_<name>" in mask
    assert f"then {builder}" in mask
    assert re.findall(MAKE_STEP, mask.split("then ", 1)[1]) == readme


def test_an_unpromoted_envs_coverage_gap_fix_generates_checks_and_rehearses_itself(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("GENIERAILS_RERUN_TARGET", raising=False)
    dev, _prod = _gap_envs(tmp_path)
    expected = ["make generate ENV=dev", "make check ENV=dev", "make rehearse ENV=dev"]
    builder = coverage_fix.recovery_steps(["dev"], "envs/dev/env.auto.tfvars", "make rehearse ENV=dev")
    assert re.findall(MAKE_STEP, builder) == expected
    assert f"Next: {builder}\n" in _scaffold_output(capsys, "dev", dev, dev)
    fix = "\n".join(coverage_fix.fix_lines("dev", dev, [("paycat.s.t.loyalty", "class.loyalty_id")]))
    assert f"then {builder}" in fix
    monkeypatch.setenv("GENIERAILS_RERUN_TARGET", "maintain")
    fix = "\n".join(coverage_fix.fix_lines("dev", dev, [("paycat.s.t.loyalty", "class.loyalty_id")]))
    assert re.findall(MAKE_STEP, fix.split("then ", 1)[1]) == expected + ["make maintain ENV=dev"]


def _chain_envs(tmp_path: Path, stg_from: str) -> tuple[Path, Path, Path]:
    """dev, stg and a prod promoted from stg; stg is promoted from ``stg_from`` (or is a root)."""
    texts = {"dev": DETERMINISTIC, "stg": DETERMINISTIC + (f'promote_from = "{stg_from}"\n' if stg_from else ""),
             "prod": DETERMINISTIC_STG}
    for name, text in texts.items():
        (tmp_path / "envs" / name).mkdir(parents=True)
        (tmp_path / "envs" / name / "env.auto.tfvars").write_text(text)
    return tuple(tmp_path / "envs" / name for name in texts)


def _all_fixes(prod: Path) -> list[str]:
    return ["\n".join(coverage_fix.fix_lines("prod", prod, [("ppay.s.t.loyalty", "class.loyalty_id")])),
            "\n".join(coverage_fix.materialize_lines("prod", prod, ["generic_partial"]))]


def test_a_prod_promoted_from_a_rehearsed_stg_maps_and_rehearses_in_stg(tmp_path, capsys, monkeypatch):
    """stg has no promote_from: it owns tag_treatments and is the env release-plan ENV=prod needs rehearsed."""
    monkeypatch.delenv("GENIERAILS_RERUN_TARGET", raising=False)
    _dev, stg, prod = _chain_envs(tmp_path, "")
    assert coverage_fix.promotion_chain(prod) == ["stg", "prod"]
    expected = ["make generate ENV=stg", "make check ENV=stg", "make rehearse ENV=stg", "make promote-to ENV=prod",
                "make check ENV=prod", "make rehearse ENV=stg", "make release-plan ENV=prod", "make release ENV=prod"]
    builder = coverage_fix.recovery_steps(["stg", "prod"], "envs/stg/env.auto.tfvars")
    assert re.findall(MAKE_STEP, builder) == expected
    for text in _all_fixes(prod):
        assert f"then {builder}" in text and "envs/stg/env.auto.tfvars" in text
        # adopt-custom-mask runs only in dev, and dev's custom_masks/ never reach a root stg.
        assert "make adopt-custom-mask ENV=dev TREATMENT=" not in text
        assert "envs/stg has no promote_from, so map a mask-library treatment there" in text
    assert f"Next: {builder}\n" in _scaffold_output(capsys, "prod", prod, stg)


def test_a_prod_promoted_through_a_promoted_stg_maps_in_dev_and_never_hand_edits_stg(tmp_path, capsys, monkeypatch):
    """promote-to ENV=stg rewrites stg's tag_treatments from dev, so dev owns the mapping (and the custom mask)."""
    monkeypatch.delenv("GENIERAILS_RERUN_TARGET", raising=False)
    dev, _stg, prod = _chain_envs(tmp_path, "dev")
    assert coverage_fix.promotion_chain(prod) == ["dev", "stg", "prod"]
    builder = coverage_fix.recovery_steps(["dev", "stg", "prod"], "envs/dev/env.auto.tfvars")
    assert re.findall(MAKE_STEP, builder) == [
        "make generate ENV=dev", "make check ENV=dev", "make rehearse ENV=dev", "make promote-to ENV=stg",
        "make check ENV=stg", "make promote-to ENV=prod", "make check ENV=prod"]
    # make rehearse refuses a promoted stg, so no rehearse of stg and no release-plan of prod is offered.
    assert "release-plan ENV=prod needs a passing rehearsal of stg, and make rehearse refuses envs/stg" in builder
    for text in _all_fixes(prod):
        assert f"then {builder}" in text
        assert "make adopt-custom-mask ENV=dev TREATMENT=custom_<name> and add its line to tag_treatments " \
            "(or column_overrides) in envs/dev/env.auto.tfvars" in text
        assert "envs/stg/env.auto.tfvars" not in text and "make generate ENV=stg" not in text
    assert f"Next: {builder}\n" in _scaffold_output(capsys, "prod", prod, dev)


def test_rehearse_reuse_docs_warn_that_live_dev_is_not_rechecked():
    for path in (ROOT / "REFERENCE.md", SHARED / "docs" / "design" / "deterministic-governance.md"):
        text = " ".join(path.read_text().split())
        assert "does **not** re-check live dev for out-of-band drift" in text
        assert "revert the live change and run a real apply/rehearse" in text
        assert "abandon/re-stage the unfinished policy change" in text
