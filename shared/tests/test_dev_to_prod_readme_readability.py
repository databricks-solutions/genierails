"""The champion walkthrough stays short; its details live in DETAILS.md and stay reachable.

README.md is the happy path a champion reads top to bottom: the 8-command
flow, one table, then each step as a goal, its command, a few bullets and a
"More:" link. Edge cases, alternatives, CI and legacy behaviour moved to
DETAILS.md, one section per step. These tests keep the README short, keep
every anchor other docs link to, keep every README link to DETAILS.md
resolvable (and every DETAILS.md section linked), and keep stale steps out of
both files.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WALKTHROUGH = ROOT / "shared" / "examples" / "dev_to_prod"
README = WALKTHROUGH / "README.md"
DETAILS = WALKTHROUGH / "DETAILS.md"

# Every anchor the README had before the rewrite: other docs, SAMPLE_ENV.md and REFERENCE.md link to them.
README_ANCHORS = ["what-each-command-asks", "prerequisites--gather-required-values", "phase-0--dev-set-up",
                  "phase-1--dev-scan-draft-and-test-rules", "dev-classify", "phase-2--prod-set-up-and-promote-rules",
                  "phase-3--prod-scan-real-data", "phase-4--prod-release-and-verify", "phase-5--prod-maintain-coverage",
                  "ci--scripted", "commit-it"]
# Heading anchors other docs link to.
README_HEADINGS = {"the-whole-flow": "## The whole flow", "prod": "## Prod", "ci--scripted": "## CI / scripted",
                   "commit-it": "## Commit it"}


def _anchors(text: str) -> set[str]:
    return set(re.findall(r'<a id="([^"]+)"></a>', text))


def _steps(text: str) -> list[str]:
    return re.findall(r"^\*\*(\d+\. [^*]+)\*\*", text, re.M)


def test_readme_keeps_every_anchor_other_docs_link_to():
    text = README.read_text()
    assert set(README_ANCHORS) <= _anchors(text), set(README_ANCHORS) - _anchors(text)
    for heading in README_HEADINGS.values():
        assert f"\n{heading}\n" in text, heading
    # Every link into README.md#... anywhere in the repo resolves.
    tracked = subprocess.run(["git", "ls-files", "*.md"], cwd=ROOT, text=True, capture_output=True, check=True)
    known = _anchors(text) | set(README_HEADINGS)
    for path in tracked.stdout.split():
        for anchor in re.findall(r"dev_to_prod/README\.md#([A-Za-z0-9_-]+)", (ROOT / path).read_text()):
            assert anchor in known, (path, anchor)
        if Path(path).parent == Path("shared/examples/dev_to_prod"):
            for anchor in re.findall(r"\]\(README\.md#([A-Za-z0-9_-]+)\)", (ROOT / path).read_text()):
                assert anchor in known, (path, anchor)
    for anchor in re.findall(r"\]\(#([A-Za-z0-9_-]+)\)", text):
        assert anchor in known, anchor


def test_every_details_link_resolves_and_every_details_section_is_linked():
    readme, details = README.read_text(), DETAILS.read_text()
    anchors = _anchors(details)
    links = re.findall(r"\]\(DETAILS\.md#([A-Za-z0-9_-]+)\)", readme)
    assert links, "the README links to no DETAILS.md section"
    for anchor in links:
        assert anchor in anchors, anchor
    # Each DETAILS.md section has an anchor right above its heading, and the README links to it.
    headings = re.findall(r'<a id="([^"]+)"></a>\n## ', details)
    assert len(headings) == details.count("\n## "), "a DETAILS.md section has no anchor"
    assert set(headings) <= set(links), set(headings) - set(links)
    # DETAILS.md's own links back into the README and itself resolve too.
    for anchor in re.findall(r"\]\(README\.md#([A-Za-z0-9_-]+)\)", details):
        assert anchor in _anchors(readme) | set(README_HEADINGS), anchor
    for anchor in re.findall(r"\]\(#([A-Za-z0-9_-]+)\)", details):
        assert anchor in anchors, anchor
    # ...and every relative file link in both files exists.
    for path, text in ((README, readme), (DETAILS, details)):
        for target in re.findall(r"\]\(([^)#:]+)(?:#[^)]*)?\)", text):
            assert (path.parent / target).exists(), (path.name, target)


def test_readme_is_the_short_happy_path():
    text = README.read_text()
    # About a 5-minute read: the rewrite took it from ~4,500 words to under 2,000.
    assert len(text.split()) < 2000, len(text.split())
    # The same steps, in the same order, each with a one-line goal after its title.
    dev = text[text.index("\n## Dev\n"):text.index("\n## Prod\n")]
    prod = text[text.index("\n## Prod\n"):text.index("\n## Keep it safe\n")]
    assert _steps(dev) == ["1. Set up.", "2. Commit it (own repo).", "3. Classify (in the UI).", "4. Generate.",
                           "5. Rehearse."]
    assert _steps(prod) == ["1. Set up and promote.", "2. Classify (in the UI).", "3. Plan, release.",
                            "4. Record evidence."]
    for line in text.splitlines():
        if re.match(r"\*\*\d+\. ", line):
            goal = line.split("**", 2)[2].strip()
            assert goal and len(goal) <= 120, line
    # No step paragraph runs on: every line of prose outside tables and code stays short.
    in_code = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code or line.startswith("|") or line.startswith("<a id="):
            continue
        assert len(line.split()) <= 70, line
    # Each step with details links to them.
    for section, titles in ((dev, ["1. Set up.", "2. Commit it (own repo).", "4. Generate.", "5. Rehearse."]),
                            (prod, ["1. Set up and promote.", "3. Plan, release.", "4. Record evidence."])):
        for title in titles:
            start = section.index(f"**{title}**")
            nxt = re.compile(r"\n\*\*\d+\. ").search(section, start + 1)
            step = section[start:nxt.start() if nxt else None]
            assert "](DETAILS.md#" in step, title


def test_neither_file_brings_back_the_removed_steps():
    readme, details = README.read_text(), DETAILS.read_text()
    flow = readme[readme.index("## The whole flow"):readme.index("## Before you start")]
    code = flow[flow.index("```bash"):flow.index("```", flow.index("```bash") + 3)]
    # No separate check, release-plan, account or git steps in the flow: the commands run or offer them.
    for gone in ("make check ENV=", "make release-plan", "account-plan", "apply-account", "git add", "git commit",
                 "git push", "git remote"):
        assert gone not in code, gone
    # The walkthrough steps never ask for a manual git add/commit/push: the commit offers do it. Only the
    # off-terminal table and the ack (no command offers it) in DETAILS.md name git commands.
    steps = readme[readme.index("\n## Dev\n"):]
    for gone in ("git add", "git commit", "git push", "git remote"):
        assert gone not in steps, gone
    assert sorted(re.findall(r"`git add [^`]+`", details)) == ["`git add .gitignore envs/`", "`git add envs/prod/ack.txt`"]
    for text in (readme, details):
        # No old evidence variable, no copy-the-templates step, no second dev rehearse on the promotion.
        assert "GENIERAILS_EVIDENCE_INTEGRATION" not in text
        for gone in ("cp ../shared/examples", "mkdir -p envs/dev/agents", "copy the env + agent templates"):
            assert gone not in text, gone
        for gone in ("rehearse dev on the promotion commit", "`make rehearse ENV=dev` again on that commit",
                     "**5. Check.**", "**6. Rehearse.**"):
            assert gone not in text, gone
    # Prod's steps name no rehearse before release: release reuses dev's rehearsal.
    prod = readme[readme.index("\n## Prod\n"):readme.index("\n## Keep it safe\n")]
    assert "make rehearse ENV=dev" not in prod[:prod.index("make release ENV=prod")]


def test_the_relocated_facts_are_in_details_by_step():
    """Each edge case the README used to carry now sits in the DETAILS.md section its step links to."""
    details = DETAILS.read_text()

    def section(title: str) -> str:
        start = details.index(f"\n## {title}\n")
        end = details.find("\n## ", start + 1)
        return details[start:end if end != -1 else None]

    expected = {
        "Your repo and remotes": ["through `MAKEFLAGS`", "an exported shell `REPO` is ignored",
                                  "`git remote rename origin genierails`", "Git URL rewrites are refused too",
                                  "a different `REPO=` once `origin` exists is refused"],
        "Dev step 1: Set up": ["**Also accepted:**", "`make split-agent-files`", "`acl_groups` is required",
                               "only tier groups and `raw_exempt_principals` can be readers",
                               'governance_mode = "deterministic"'],
        "Dev step 2: Commit it": ["option-A local-only repo", "a rehearse receipt counts only for a clean checkout"],
        "Dev step 4: Generate": ["`COMMIT=0`", "- **Deterministic**", "- **Legacy**", "`column_overrides`"],
        "Dev step 5: Rehearse": ["`GENIERAILS_INTERACTIVE=0`", "`no resource changes`", "`make policy-abandon ENV=dev`",
                                 "`verify_key_columns`", "`ERROR: account plan is non-empty`"],
        "Prod step 1: Set up and promote": ["`REUSE_SOURCE_AUTH=1", "`allow_identity_catalog_map = true`",
                                            "`enable_classification = true`", "`--skip-agent`",
                                            "a second `catalog_map` line is refused"],
        "Prod step 3: Plan, release": ["recorded (OS user, git identity, host, terminal, time)",
                                       "`ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,...`", "`promote_from = \"stg\"`",
                                       "Legacy envs skip `make release-plan`", "at most 8 rounds",
                                       "`ERROR: unacknowledged reader prod:reader:<table>:<principal>: ...`",
                                       "reports them as stale"],
        "If prod stops on coverage or drift": ["**Reuse an existing mask**", "**A new kind of mask**",
                                               "`make draft-custom-mask`", "legacy-mode only"],
        "Prod step 4: Record evidence": ["`$GENIERAILS_STATE_DIR`", "`WAREHOUSE_ID=<id>` only pins another",
                                         "`OFFLINE=1`"],
        "Keep it safe": ["never changes the Genie agent", "on a clean checkout"],
        "CI / scripted": ["`COMMIT=1`", "`make release ENV=prod MANIFEST=<path>`", "second person"],
        "Commit it": ["`git push -u origin main`", "`make next ENV=dev` names that push"],
    }
    for title, facts in expected.items():
        text = section(title)
        for fact in facts:
            assert fact in text, (title, fact)
