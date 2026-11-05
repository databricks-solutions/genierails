"""The walkthrough's Prod section, word for word, in a customer repo, through release-plan.

The repo starts as a GenieRails clone (the upstream root .gitignore) with
its own origin, like the walkthrough's "your own repo first". Dev is
committed and rehearsed at an earlier commit (dev step 5). Then every command
in shared/examples/dev_to_prod/README.md's Prod section runs in order, read
from the README itself, up to make release ENV=prod: setup, promote-to,
the commit offer and release, which at a terminal runs release-plan (and
make check) and stops at its first y/N, answered n here.

setup and promote-to run the real recipes (Terraform is a stub); rehearse
and release-plan run with tests/test_release_gate_make.py's stubbed
recursive make, check and live checks, so nothing reaches Databricks.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import release_gate as rg
from tests.test_check_target import FAKE_TERRAFORM, _env
from tests.test_commit_envs import _pty_run
from tests.test_deterministic_check_promote import DEPLOYER, DEV_ENV, LEGACY_ACCOUNT, TIERS, _auth, _write_generated
from tests.test_release_gate import git_env
from tests.test_release_gate_make import Make

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
README = SHARED / "examples" / "dev_to_prod" / "README.md"
# The walkthrough's details and edge cases, one section per README step.
DETAILS = README.parent / "DETAILS.md"

if shutil.which("make") is None or shutil.which("git") is None:
    pytest.skip("make and git are required", allow_module_level=True)


def _section(title: str) -> str:
    text = README.read_text()
    start = text.index(f"\n## {title}\n")
    end = text.find("\n## ", start + 1)
    return text[start:end if end != -1 else None]


def _details(title: str) -> str:
    """A DETAILS.md section, by its heading; the README step it details links to it."""
    text = DETAILS.read_text()
    start = text.index(f"\n## {title}\n")
    end = text.find("\n## ", start + 1)
    section = text[start:end if end != -1 else None]
    anchor = re.search(r'<a id="([^"]+)"></a>\s*$', text[:start])
    assert anchor, f"DETAILS.md section {title!r} has no anchor"
    return section


def _links_to_details(readme_part: str, title: str) -> None:
    """The README part links to the DETAILS.md section, by the section's own anchor."""
    text = DETAILS.read_text()
    start = text.index(f"\n## {title}\n")
    anchor = re.search(r'<a id="([^"]+)"></a>\s*$', text[:start]).group(1)
    assert f"](DETAILS.md#{anchor})" in readme_part, (title, anchor)


def prod_commands() -> list[str]:
    """Each command the Prod section tells the user to run, in order, up to release-plan.

    Commands are the lines of its bash blocks plus the inline commit
    instruction ("Commit and push ... (`git add ...`, commit, `git push`)"), or
    the answer to promote-to's commit offer ("It then offers to commit and push
    ...: answer `y`"), which is "answer y".
    """
    section = _section("Prod")
    commands = []
    for match in re.finditer(r"```bash\n(.*?)```|Commit and push[^\n]*?\(`(git add [^`]+)`, commit, `(git push)`\)"
                             r"|It then offers to commit and push[^\n]*?: answer `(y)`", section, re.S):
        if match.group(1):
            commands += [line.split("#")[0].strip() for line in match.group(1).splitlines()
                         if line.split("#")[0].strip()]
        elif match.group(4):
            commands.append("answer y")
        else:
            commands += [match.group(2), "git commit", match.group(3)]
    assert "make release ENV=prod" in commands, commands
    return commands[:commands.index("make release ENV=prod") + 1]


class Customer:
    """A GenieRails clone with its own origin; dev committed and rehearsed."""

    def __init__(self, tmp_path: Path):
        self.tmp = tmp_path
        self.root = tmp_path / "repo"
        self.cloud = self.root / "aws"
        self.origin = tmp_path / "origin.git"
        self.env = git_env(tmp_path) | {"GIT_CONFIG_NOSYSTEM": "1"}
        os.environ.pop(rg.STATE_DIR_ENV, None)
        self.cloud.mkdir(parents=True)
        shutil.copy2(ROOT / ".gitignore", self.root / ".gitignore")
        shutil.copy2(ROOT / "aws" / ".gitignore", self.cloud / ".gitignore")
        shutil.copy2(ROOT / "aws" / "Makefile", self.cloud / "Makefile")
        # A real copy: release_gate fingerprints and status-checks shared/ inside the repo.
        shutil.copytree(SHARED, self.root / "shared", symlinks=True,
                        ignore=shutil.ignore_patterns("tests", "__pycache__", ".pytest_cache", "logs"))
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
        (bin_dir / "terraform").chmod(0o755)
        self.git("init", "-q", "-b", "main")
        # Like a real clone, the engine files GenieRails tracks are tracked here too, the ones the
        # root .gitignore matches (treatment_config.json, example masking_functions.sql) included:
        # release_gate refuses an ignored engine file, which the code fingerprint can't see.
        listed = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "--", "shared"],
                                capture_output=True, check=True).stdout.split(b"\0")
        engine = b"\0".join(rel for rel in listed if rel and (self.root / os.fsdecode(rel)).is_file())
        subprocess.run(["git", "-C", str(self.root), "add", "-f", "--pathspec-from-file=-", "--pathspec-file-nul"],
                       input=engine, check=True, capture_output=True, env=os.environ | self.env)
        subprocess.run(["git", "init", "-q", "--bare", str(self.origin)], check=True, env=os.environ | self.env)
        self.git("remote", "add", "origin", str(self.origin))
        self.stubbed = Make(tmp_path, self)

    def git(self, *args: str, cwd: Path | None = None) -> str:
        result = subprocess.run(["git", "-C", str(cwd or self.root), *args], text=True, capture_output=True,
                                env=os.environ | self.env)
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout.strip()

    def make(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["make", "--no-print-directory", *args], cwd=self.cloud, text=True,
                              capture_output=True, env=_env(self.tmp, **self.env), timeout=600)

    def stub_make(self, target: str, env: str) -> subprocess.CompletedProcess:
        return self.stubbed(target, env, cwd=self.cloud)

    def head(self) -> str:
        return self.git("rev-parse", "HEAD")


@pytest.fixture
def customer(tmp_path, monkeypatch):
    monkeypatch.delenv(rg.STATE_DIR_ENV, raising=False)
    repo = Customer(tmp_path)
    # Dev: set up, its rules generated and committed (the whole flow's first commit).
    result = repo.make("setup", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    dev = repo.cloud / "envs" / "dev"
    (dev / "env.auto.tfvars").write_text(DEV_ENV)
    (dev / "agents" / "Finance Analytics.auto.tfvars").unlink()  # DEV_ENV lists its agent inline
    (dev / "auth.auto.tfvars").write_text(_auth(DEPLOYER))
    (repo.cloud / "envs" / "account" / "abac.auto.tfvars").write_text(LEGACY_ACCOUNT)
    (dev / "generated").rmdir()  # setup's empty dir; _write_generated writes generate's output
    _write_generated(dev)
    result = repo.make("promote", "ENV=dev")
    assert result.returncode == 0, result.stdout + result.stderr
    # The rest of what make generate writes.
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(dev), "--config", str(dev / "generated" / "abac.auto.tfvars")], check=True)
    repo.git("add", "-A")
    repo.git("commit", "-q", "-m", "Govern the first agent in dev")
    repo.git("push", "-q", "-u", "origin", "main")
    assert repo.git("status", "--porcelain") == ""
    # Dev step 5.
    result = repo.stub_make("rehearse", "dev")
    assert result.returncode == 0, result.stdout + result.stderr
    assert rg.passing_receipt(repo.cloud, "dev", repo.head())
    return repo


def _fill_in_prod(repo: Customer) -> None:
    """What Prod step 1 asks for between setup and promote-to."""
    prod = repo.cloud / "envs" / "prod"
    (prod / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))
    env_file = prod / "env.auto.tfvars"
    text = env_file.read_text()
    text = re.sub(r'deployer_principal_id\s*=\s*"[^"]*"', f'deployer_principal_id = "{DEPLOYER}"', text)
    text = re.sub(r"access_tier_groups\s*=\s*\[[^\]]*\]", f"access_tier_groups = {TIERS!r}".replace("'", '"'), text)
    text, found = re.subn(r"^catalog_map\s*=.*$", 'catalog_map = { "paycat" = "ppay" }', text, flags=re.M)
    env_file.write_text(text if found else text + 'catalog_map = { "paycat" = "ppay" }\n')


def test_the_prod_section_reads_as_expected():
    assert prod_commands() == [
        "make setup ENV=prod", "make promote-to ENV=prod", "answer y",
        "make release ENV=prod",
    ]
    # The whole flow's promote-to commits the same files itself.
    flow = _section("The whole flow")
    assert "make promote-to ENV=prod    # copies dev's proven rules to prod; offers to commit + push (y)" \
        in flow.splitlines()
    assert "Promote the rules to prod" not in flow
    # ...the README's step names the files that offer commits...
    step = _step(_section("Prod"), "2. Copy the proven rules.")
    assert "- It then offers to commit and push `.gitignore`, `envs/prod/` and `envs/account/`: answer `y`." \
        in step.splitlines()
    assert "| `make promote-to ENV=prod` | The same commit question | Same: save this step's files |" \
        in _section("Reference").splitlines()
    # ...and so does the full table in DETAILS.md.
    assert "| `make promote-to ENV=prod` | Copies dev's reviewed rules into prod, renamed to prod's catalogs and " \
        "groups; prints the changed files by owner | The same commit question, for `.gitignore`, `envs/prod/` and " \
        "`envs/account/` |" in _details("What each command does, in full").splitlines()


def _step(section: str, title: str) -> str:
    start = section.index(f"**{title}**")
    end = re.compile(r"\n\*\*\d+\. ").search(section, start + 1)
    return section[start:end.start() if end else None]


def test_prod_step_1_says_to_replace_the_seeded_catalog_map_line():
    # make setup ENV=prod seeds catalog_map with placeholders; a second line is refused.
    step = _step(_section("Prod"), "1. Set up.")
    assert "replace the `deployer_principal_id` placeholder and the seeded `catalog_map` line" in step
    assert "placeholder, and set:" not in step
    assert "Setup makes no agent file for prod: `promote-to` writes prod's." in step
    details = _details("Prod steps 1-2: Set up and copy the rules")
    # Steps 1 and 2 share one DETAILS.md section; step 2 (promote-to) links to it.
    _links_to_details(_step(_section("Prod"), "2. Copy the proven rules."), "Prod steps 1-2: Set up and copy the rules")
    assert "replace its `deployer_principal_id` placeholder and the seeded `catalog_map` line" in details
    assert "a second `catalog_map` line is refused" in details
    assert "placeholder, and set:" not in details
    makefile = (SHARED / "Makefile.shared").read_text()
    assert 'catalog_map  = { "<$(DEFAULT_ENV)_catalog>" = "<%s_catalog>" }' in makefile
    assert "catalog_map is set twice; keep one" in (SHARED / "genie_space_placeholder.py").read_text()


RELEASE_PHASE_HEADING = "=== Release phase applied (prod) ==="
# Each round's own line, in the recipe's order: import-only, a staged policy phase, the SELECT round.
RELEASE_ROUND_EXPLANATIONS = [
    "This manifest imported existing resources into Terraform state; nothing else was applied.",
    "This manifest held one phase of a staged policy change; it is applied and verified.",
    "This manifest opened the SELECT the Genie agents need; the workspace (CAN_RUN) is planned with it live.",
]
RELEASE_NEXT = "Next: make release-plan ENV=prod, approve the new manifest, make release ENV=prod."
RELEASE_COMPLETE = "=== Release complete (prod) ==="


def _recipe_echo_lines() -> list[str]:
    """The echoed strings of _release-manifest, in recipe order, with ENV=prod."""
    makefile = (SHARED / "Makefile.shared").read_text()
    start = makefile.index("\n_release-manifest:")
    recipe = makefile[start:makefile.index("\n\n", start)]
    return [match.group(1).replace("$(ENV)", "prod")
            for match in re.finditer(r'^\s*echo "([^"]*)"', recipe, re.M)]


def test_the_release_recipe_prints_heading_then_explanation_then_next_per_round():
    lines = [line.strip() for line in _recipe_echo_lines()]
    headings = [i for i, line in enumerate(lines) if line == RELEASE_PHASE_HEADING]
    assert len(headings) == 3, lines
    for heading, explanation in zip(headings, RELEASE_ROUND_EXPLANATIONS):
        assert lines[heading + 1:heading + 3] == [explanation, RELEASE_NEXT], lines[heading:heading + 3]
    assert lines.index(RELEASE_COMPLETE) > headings[-1]


def test_prod_step_4_says_a_first_release_takes_several_rounds_in_the_real_messages():
    step = _step(_section("Prod"), "4. Release.")
    # The README step: why several rounds (masks first, access last), y each, until the recipe's own end line.
    assert f"*Why several rounds:* a first release goes masks first, access last: the masks, then the column tags, " \
        f"then read access, then access to the agent. Each round is proven before the next opens more. Answer `y` each round " \
        f"until it prints `{RELEASE_COMPLETE}`." in step.splitlines()
    _links_to_details(step, "Prod step 4: Release")
    details = _details("Prod step 4: Release")
    rounds = details[details.index("**A first release takes several rounds.**"):]
    rounds = rounds[:rounds.index("\n")]
    # The README shows one round's output in the recipe's order: heading, what it applied, Next;
    # then the end, "Release complete".
    def at(message: str) -> int:
        assert rounds.count(f"`{message}`") == 1, message
        return rounds.index(f"`{message}`")
    heading, explanations, after = at(RELEASE_PHASE_HEADING), [at(m) for m in RELEASE_ROUND_EXPLANATIONS], at(RELEASE_NEXT)
    assert heading < explanations[0] and explanations == sorted(explanations) and explanations[-1] < after, rounds
    assert after < rounds.index(f"until `make release` prints `{RELEASE_COMPLETE}`"), rounds
    assert "Every round but the last prints `=== Release phase applied (prod) ===`, then a line saying what it applied" in rounds
    # create (functions, policies) -> switch (column tags) -> grant (USE, SELECT) -> deferred workspace (CAN_RUN).
    order = ["masking functions and policies", "column tags", "`SELECT`", "Genie `CAN_RUN`"]
    assert [rounds.index(part) for part in order] == sorted(rounds.index(part) for part in order)
    assert "planned only once that `SELECT` is live" in rounds
    # The reference's release row and the whole flow say so too.
    reference = (README.parent / "REFERENCE.md").read_text()
    row = next(line for line in reference.splitlines()
               if line.startswith("| `make release ENV=prod` | 4 | (deterministic prod)"))
    assert "A first release takes several rounds (an import-only manifest if needed, create, switch, grant, " \
        "then the deferred workspace)" in row
    positions = [row.index(f"`{message}`") for message in (RELEASE_PHASE_HEADING, RELEASE_NEXT, RELEASE_COMPLETE)]
    assert positions == sorted(positions), row
    flow = _section("The whole flow")
    assert 'make release ENV=prod       # applies prod in rounds; shows each round\'s plan, asks y/N; ends at ' \
        '"Release complete"' in flow.splitlines()
    # The flow's commands have no release-plan line: release runs it each round, which the table says,
    # with the exact question it asks.
    code = flow[flow.index("```bash"):flow.index("```", flow.index("```bash") + 3)]
    assert "make release-plan" not in code and "repeats release-plan + release" not in _section("Prod")
    row = next(line for line in _section("Reference").splitlines() if line.startswith("| `make release ENV=prod` |"))
    assert "Each round: exactly that round's prod changes" in row
    assert "`Approve and release manifest <id> (commit <sha>, round <n> of at most 8)? [y/N]`, once per round" in row
    full = next(line for line in _details("What each command does, in full").splitlines()
                if line.startswith("| `make release ENV=prod` |"))
    assert "Per round: `make release-plan` (runs `make check ENV=prod`, reuses dev's rehearsal" in full
    assert "`Approve and release manifest <id> (commit <sha>, round <n> of at most 8)? [y/N]`, once per round" in full


def test_prod_step_3_tells_legacy_envs_to_skip_release_plan():
    step = _details("Prod step 4: Release")
    line = "Legacy envs skip `make release-plan`: run `make release ENV=prod` directly " \
        "(`release-plan` refuses them with `Legacy envs keep: make release ENV=prod`)."
    assert line in step.splitlines()
    makefile = (SHARED / "Makefile.shared").read_text()
    recipe = makefile[makefile.index("\nrelease-plan:"):]
    assert "Legacy envs keep: make release ENV=$(ENV)" in recipe[:recipe.index("_DEPLOYER_CHECK")]


def test_following_the_prod_section_word_for_word_reaches_a_release_manifest(customer):
    repo = customer
    out = {}
    commands = prod_commands()
    for i, command in enumerate(commands):
        if command == "answer y":
            continue  # typed at the promote-to run before it
        words = shlex.split(command)
        if words[0] == "git":
            if words[1] == "commit":
                words = ["git", "commit", "-q", "-m", "Promote the rules to prod"]
            out[command] = repo.git(*words[1:], cwd=repo.cloud)  # the walkthrough runs from aws/
            continue
        target, env = words[1], words[2].split("=", 1)[1]
        if target in ("rehearse", "release-plan"):
            result = repo.stub_make(target, env)
        elif target == "release":
            # At a terminal: the first round's release-plan runs (stubbed below make), then n at its y/N.
            # Dev step 5's rehearse applied the account layer, so release's account step finds no changes.
            planning = Make(repo.tmp, repo, passthrough=("release-plan",))
            args, environ = planning.command("release", env, root_runner=planning.empty_account_runner())
            code, tty_out = _pty_run(args, "n", environ, cwd=repo.cloud)
            result = subprocess.CompletedProcess(command, 0 if "Not released. Manifest" in tty_out else code,
                                                 tty_out, "")
            assert code != 0 and "Approve and release manifest" in tty_out, tty_out
        elif commands[i + 1:i + 2] == ["answer y"]:
            code, tty_out = _pty_run(["make", "--no-print-directory", target, f"ENV={env}"], "y",
                                     _env(repo.tmp, **repo.env), cwd=repo.cloud)
            result = subprocess.CompletedProcess(command, code, tty_out, "")
            assert "Commit these changes and push them to origin/main? [y/N]" in tty_out, tty_out
            assert "Pushed to origin/main." in tty_out, tty_out
        else:
            result = repo.make(target, f"ENV={env}")
        text = out[command] = result.stdout + result.stderr
        if target == "setup":
            # make setup ENV=prod adds prod's lines to the .gitignore the next commit must include.
            assert "/envs/prod/abac.auto.tfvars" in (repo.cloud / ".gitignore").read_text()
            shown = re.findall(r"make [a-z-]+ ENV=prod", text)
            assert shown[-5:] == ["make promote-to ENV=prod", "make enable-classification ENV=prod",
                                  "make release ENV=prod", "make evidence ENV=prod",
                                  "make maintain ENV=prod"], shown
            assert "make rehearse ENV=" not in text and "make release-plan ENV=prod" not in text
            _fill_in_prod(repo)
        elif target == "check":
            # Offline, check's plan step (6) needs prod's live classification config.
            assert "OK: promote-to ENV=prod CHECK=1: committed files match." in text, text
            assert "OK: generated files (envs/prod): committed files match." in text, text
            assert result.returncode == 0 or "=== make check ENV=prod: FAILED (plan) ===" in text, text
        else:
            assert result.returncode == 0, f"{command}:\n{text}"

    promoted = repo.head()
    assert repo.git("status", "--porcelain") == ""
    # No second make rehearse ENV=dev: release-plan reused dev's rehearsal for the promotion commit.
    plan_out = out["make release ENV=prod"]
    assert "reusing dev's passing rehearsal of" in plan_out and "without re-applying" in plan_out, plan_out
    assert rg.passing_receipt(repo.cloud, "dev", promoted)["applied"] is False
    manifest = rg.current_manifest(repo.cloud / "envs" / "prod", "prod")
    assert manifest and manifest.is_file(), out["make release ENV=prod"]
    assert f"Manifest: {manifest}" in plan_out and not rg.release_receipts(repo.cloud, "prod")


def test_the_ask_table_quotes_the_questions_the_code_asks():
    """Each y/N the walkthrough's table promises is the exact question its command asks."""
    reference = _section("Reference")
    asks = reference[reference.index("### What each command asks"):reference.index("### Words used on this page")]
    rows = {line.split("|")[1].strip(): line for line in asks.splitlines() if line.startswith("| `make ")}
    full = {line.split("|")[1].strip(): line for line in _details("What each command does, in full").splitlines()
            if line.startswith("| `make ")}
    # The README lists every command of the full table in DETAILS.md that asks something, in the same order,
    # with the same questions; the commands that ask nothing are named in one line.
    quiet = [command for command, line in full.items() if line.rstrip().endswith("| nothing |")]
    assert quiet == ["`make evidence ENV=prod`", "`make maintain ENV=prod`",
                     "`make next ENV=<env>` / `make status ENV=<env>`"]
    assert list(rows) == [command for command in full if command not in quiet]
    assert "`evidence`, `maintain`, `next` and `status` ask nothing." in asks
    for command, row in rows.items():
        questions = re.findall(r"`[^`]*\[y/N\]`", row)
        assert questions == re.findall(r"`[^`]*\[y/N\]`", full[command]), command
        # The README's second column is what it asks, its third what a y approves.
        cells = [cell.strip() for cell in row.strip().strip("|").split(" | ")]
        assert len(cells) == 3 and cells[1] != "nothing" and cells[2] not in ("", "—"), row
        assert questions or cells[1] == "The same commit question", row
    assert "](DETAILS.md#what-each-command-does-in-full)" in asks
    assert list(rows) == ["`make setup ENV=dev REPO=<url>`", "`make generate ENV=dev`", "`make rehearse ENV=dev`",
                          "`make setup ENV=prod`", "`make promote-to ENV=prod`", "`make release ENV=prod`"]
    commit_envs = (SHARED / "scripts" / "commit_envs.py").read_text()
    assert "`Commit these changes and push them to origin/main? [y/N]`" in rows["`make setup ENV=dev REPO=<url>`"]
    assert "`... and keep them local (no origin remote)? [y/N]`" in rows["`make setup ENV=dev REPO=<url>`"]
    assert 'input(f"Commit these changes and {target}? [y/N] ")' in commit_envs
    assert 'f"push them to {remote}/{branch}" if has_remote else f"keep them local (no {remote} remote)"' in commit_envs
    assert "`Approve and apply this account manifest? [y/N]`" in rows["`make rehearse ENV=dev`"]
    assert "override _ACCOUNT_APPROVE_QUESTION = Approve and apply this account manifest?" in \
        (SHARED / "Makefile.shared").read_text()
    assert "`Approve and release manifest <id> (commit <sha>, round <n> of at most 8)? [y/N]`" in \
        rows["`make release ENV=prod`"]
    loop = (SHARED / "scripts" / "release_loop.py").read_text()
    assert 'question = (f"Approve and release manifest {manifest[\'id\']} (commit {manifest[\'commit\'][:12]}, round "' in loop
    assert 'f"{round_no} of at most {MAX_ROUNDS})?")' in loop and "MAX_ROUNDS = 8" in loop
    # Acks stay a committed file, never a question.
    release = _step(_section("Prod"), "4. Release.")
    assert "It's your sign-off, never a `y/N`, so no command commits it: commit and push it yourself" in release


def test_release_describes_the_account_step_before_round_1_as_the_code_runs_it():
    """make release at a terminal asks about a non-empty account plan first (rehearse's step), then round 1."""
    asks = _section("Reference")
    row = next(line for line in asks.splitlines() if line.startswith("| `make release ENV=prod` |"))
    step = _step(_section("Prod"), "4. Release.")
    assert "- Before round 1, if the account plan has changes, it asks the same account question as rehearse." \
        in step.splitlines()
    assert row.index("`Approve and apply this account manifest? [y/N]` first, only when the account plan has "
                     "changes") < row.index("`Approve and release manifest <id>")
    full = next(line for line in _details("What each command does, in full").splitlines()
                if line.startswith("| `make release ENV=prod` |"))
    assert "Before round 1 it applies the account layer when it has changes, as rehearse does" in full
    assert full.index("`Approve and apply this account manifest? [y/N]` first, only when the account plan has "
                      "changes") < full.index("`Approve and release manifest <id>")
    step = _details("Prod step 4: Release")
    account = step[step.index("Before round 1 it checks the shared account layer"):]
    account = account[:account.index("\n")]
    assert step.index("Each round it plans") < step.index("Before round 1 it checks the shared account layer")
    for part in ("its sha256", "`Approve and apply this account manifest? [y/N]`", "louder warning",
                 "through `make apply-account`'s path, under the account lock", "provenance included",
                 "then round 1 starts", "Anything else applies nothing and stops before round 1",
                 "An empty account plan asks nothing."):
        assert part in account, part
    ci = next(line for line in _details("CI / scripted").splitlines() if line.startswith("| `make release ENV=prod` |"))
    assert "never asks about the account layer (`release-plan` refuses a non-empty account plan)" in ci
    assert ci.index("`make account-plan`") < ci.index("`make apply-account`") < ci.index("`make release-plan ENV=prod`")
    reference = (ROOT / "REFERENCE.md").read_text()
    release = next(line for line in reference.splitlines() if line.startswith("| `make release` |"))
    assert "`ALLOW_TAG_VALUE_REMOVAL` (at a terminal)" in release
    assert release.index("Before round 1, when the account plan has changes") \
        < release.index("`Approve and apply this account manifest? [y/N]`") \
        < release.index("with the same per-run nonce as every round's") \
        < release.index("anything else applies nothing and stops before any `release-plan`") \
        < release.index("Then each round")
    assert "a deterministic `release` asks the same account question first (before round 1)" in reference
    # ...as the code does: the release recipe runs rehearse's own account step before the loop.
    makefile = (SHARED / "Makefile.shared").read_text()
    recipe = makefile[makefile.index("\nrelease:"):]
    recipe = recipe[:recipe.index("\n\n")]
    assert recipe.index("$(call _ACCOUNT_APPROVE_APPLY,release,run)") < recipe.index("release_loop.py")
    gate = (SHARED / "scripts" / "release_gate.py").read_text()
    assert 'raise Refused(f"the account plan is not empty; rehearse and release may only add treatment-tag values. "' \
        in gate


def test_the_ci_section_names_the_low_level_commands_and_what_they_do_off_a_terminal():
    # The README's CI section says the rule and links to the low-level steps in DETAILS.md.
    short = _section("CI / scripted")
    assert '<a id="ci--scripted"></a>' in README.read_text()
    assert "no flag or variable answers an approval `y` for you" in short
    _links_to_details(short, "CI / scripted")
    section = _details("CI / scripted")
    rows = [line for line in section.splitlines() if line.startswith("| ") and not line.startswith("| Champion")
            and not line.startswith("|---")]
    assert [row.split("|")[1].strip() for row in rows] == [
        "`make setup`, `make generate`, `make promote-to`", "`make rehearse ENV=dev`", "`make release ENV=prod`",
        "(inside `rehearse` and `release-plan`)"]
    commit, rehearse, release, check = rows
    assert "No commit offer" in commit and "`git add .gitignore envs/`, commit, `git push`" in commit
    assert "Stops with `ERROR: account plan is non-empty`" in rehearse
    assert "`make account-plan`" in rehearse and "then `make apply-account`" in rehearse
    assert rehearse.index("`make account-plan`") < rehearse.index("`make apply-account`")
    assert "Releases the current manifest once (one round)" in release
    assert release.index("`make release-plan ENV=prod`") < release.index("`make release ENV=prod MANIFEST=<path>`") \
        < release.index("`=== Release complete (prod) ===`")
    assert "`make check ENV=<env>`: the read-only PR check" in check
    assert "no flag or variable answers an approval `y` for you" in section
    assert "`GENIERAILS_INTERACTIVE=0` is the one switch that turns off both approval questions and commit offers" in section
    assert "An explicit `COMMIT=1` still commits and pushes without asking" in section
    # ...as the code does: the account refusal, and release with MANIFEST= (or off a terminal) is one release.
    makefile = (SHARED / "Makefile.shared").read_text()
    assert 'echo "ERROR: account plan is non-empty; rehearse/release may only add allowed treatment-tag values."' in makefile
    release_recipe = makefile[makefile.index("\nrelease:"):]
    release_recipe = release_recipe[:release_recipe.index("\n\n")]
    assert 'if [ "$(origin MANIFEST)" = undefined ]; then' in release_recipe
    assert "INTERACTIVE_ENV = \"GENIERAILS_INTERACTIVE\"" in (SHARED / "scripts" / "approval.py").read_text()
    assert 'environ.get("GENIERAILS_INTERACTIVE") == "0"' in (SHARED / "scripts" / "commit_envs.py").read_text()


def test_the_walkthrough_says_setup_without_repo_keeps_the_genierails_remote():
    """Setup without REPO= keeps the canonical genierails remote, and that still deploys local main."""
    readme = _section("Before you start")
    assert "No repo of your own yet? Leave `REPO=` off; everything then stays in local git." in readme
    _links_to_details(readme, "Your repo and remotes")
    before = _details("Your repo and remotes")
    assert "setup renames `origin` to `genierails` and adds no `origin`" in before
    assert "only that canonical `genierails` upgrade remote, deploys from its committed local `main`" in before
    for text in (readme, before):
        assert "`git remote remove genierails`" not in text
        assert "(no remote, no GitHub account)" not in text
    # ...because provenance takes the local-only path for no remotes or exactly the canonical upgrade remote.
    gate = (SHARED / "scripts" / "release_gate.py").read_text()
    assert "def local_main_mode(" in gate and 'remotes != (UPSTREAM_REMOTE,)' in gate
    assert 'UPSTREAM_REMOTE = "genierails"' in (SHARED / "scripts" / "setup_repo.py").read_text()
