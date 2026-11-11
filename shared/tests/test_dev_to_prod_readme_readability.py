"""The champion walkthrough stays short; its details live in DETAILS.md and stay reachable.

README.md is the happy path a champion reads top to bottom: a short opening
(what GenieRails does, what you end up with, the journey), the 8-command flow,
then each step in order as a goal, its command, a few bullets, a just-in-time
explanation where a concept (commit, manifest, ack, y/N) first appears, and a
"More:" link. The ask table and a small glossary close the page as reference. Edge cases, alternatives, CI and legacy behaviour moved to
DETAILS.md, one section per step. These tests keep the README short, keep
every anchor other docs link to, keep every README link to DETAILS.md
resolvable (and every DETAILS.md section linked), and keep stale steps out of
both files.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WALKTHROUGH = ROOT / "shared" / "examples" / "dev_to_prod"
README = WALKTHROUGH / "README.md"
DETAILS = WALKTHROUGH / "DETAILS.md"

# Every anchor the README had before the rewrite: other docs, SAMPLE_ENV.md and REFERENCE.md link to them.
README_ANCHORS = ["how-it-works", "glossary", "later-changes", "what-each-command-asks", "prerequisites--gather-required-values", "phase-0--dev-set-up",
                  "phase-1--dev-scan-draft-and-test-rules", "dev-classify", "phase-2--prod-set-up-and-promote-rules",
                  "phase-3--prod-scan-real-data", "phase-4--prod-release-and-verify", "phase-5--prod-maintain-coverage",
                  "ci--scripted", "commit-it"]
# Heading anchors other docs link to.
README_HEADINGS = {"the-whole-flow": "## The whole flow", "prod": "## Prod", "ci--scripted": "## CI / scripted"}


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
    # About a 5-minute read: the rewrite took it from ~4,500 words to under 2,000. The plain-language passes
    # (just-in-time explanations of commits, manifests, acks and each y/N, an ack.txt example, the closing
    # glossary) added words for first-time readers; the cap is 2,400, no further.
    assert len(text.split()) < 2400, len(text.split())
    # The steps in the order a champion runs them, each with a one-line goal after its title.
    dev = text[text.index("\n## Dev\n"):text.index("\n## Prod\n")]
    prod = text[text.index("\n## Prod\n"):text.index("\n## Keep it healthy\n")]
    assert _steps(dev) == ["1. Set up.", "2. Review classification (in Catalog Explorer).", "3. Generate.",
                           "4. Rehearse."]
    assert _steps(prod) == ["1. Set up.", "2. Copy the proven rules.", "3. Review classification (in Catalog Explorer).",
                            "4. Release.", "5. Record evidence."]
    # The page's sections, in order: the flow map first, the reference (ask table, glossary) last.
    order = ["\n## The whole flow\n", "\n## Before you start\n", "\n## Dev\n", "\n## Prod\n",
             "\n## Keep it healthy\n", "\n## Later changes\n", "\n## CI / scripted\n", "\n## Reference\n",
             "\n### What each command asks\n", "\n### Words used on this page\n"]
    assert [text.index(h) for h in order] == sorted(text.index(h) for h in order)
    assert re.findall(r"^##+ .+$", text, re.M) == [h.strip() for h in order]
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
    for section, titles in ((dev, ["1. Set up.", "2. Review classification (in Catalog Explorer).", "3. Generate.",
                                   "4. Rehearse."]),
                            (prod, ["2. Copy the proven rules.", "4. Release.", "5. Record evidence."])):
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
    prod = readme[readme.index("\n## Prod\n"):readme.index("\n## Keep it healthy\n")]
    assert "make rehearse ENV=dev" not in prod[:prod.index("make release ENV=prod")]
    # The old up-front explainer and glossary-first layout stay gone.
    for gone in ("## How it works in 1 minute", "**Why commit.**", "**Why a manifest.**", "**Masks first, access last.**",
                 "**2. Commit it (own repo).**", "**3. Plan, release.**", "\n## Keep it safe\n", "\n## Commit it\n"):
        assert gone not in readme, gone


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
        "Dev step 1: The commit offer": ["option-A local-only repo", "a rehearse receipt counts only for a clean checkout"],
        "Dev step 3: Generate": ["`COMMIT=0`", "- **Deterministic**", "- **Legacy**", "`column_overrides`"],
        "Dev step 4: Rehearse": ["`GENIERAILS_INTERACTIVE=0`", "`no resource changes`", "`make policy-abandon ENV=dev`",
                                 "`verify_key_columns`", "`ERROR: account plan is non-empty`"],
        "Prod steps 1-2: Set up and copy the rules": ["`REUSE_SOURCE_AUTH=1", "`allow_identity_catalog_map = true`",
                                            "`enable_classification = true`", "`--skip-agent`",
                                            "a second `catalog_map` line is refused"],
        "Prod step 4: Release": ["recorded (OS user, git identity, host, terminal, time)",
                                       "`ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,...`", "`promote_from = \"stg\"`",
                                       "Legacy envs skip `make release-plan`", "at most 8 rounds",
                                       "`ERROR: unacknowledged reader prod:reader:<table>:<principal>: ...`",
                                       "reports them as stale"],
        "If prod stops on coverage or drift": ["**Reuse an existing mask**", "**A new kind of mask**",
                                               "`make draft-custom-mask`", "legacy-mode only"],
        "Prod step 5: Record evidence": ["`$GENIERAILS_STATE_DIR`", "`WAREHOUSE_ID=<id>` only pins another",
                                         "`OFFLINE=1`"],
        "Keep it healthy": ["never changes the Genie agent", "on a clean checkout"],
        "CI / scripted": ["`COMMIT=1`", "`make release ENV=prod MANIFEST=<path>`", "second person"],
        "Commit it": ["`git push -u origin main`", "`make next ENV=dev` names that push"],
    }
    for title, facts in expected.items():
        text = section(title)
        for fact in facts:
            assert fact in text, (title, fact)


def _prose(text: str) -> str:
    """The README's prose only: no code blocks, inline code, tables, anchors or link targets."""
    text = re.sub(r"```.*?```", "", text, flags=re.S)
    lines = [line for line in text.splitlines() if not line.startswith("|") and not line.startswith("<a id=")]
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", "\n".join(lines))
    return re.sub(r"`[^`]*`", "", text)


def _step(section: str, title: str) -> str:
    start = section.index(f"**{title}")
    nxt = re.compile(r"\n\*\*\d+\. ").search(section, start + 1)
    return section[start:nxt.start() if nxt else None]


def test_readme_opens_with_what_the_flow_is_for_and_explains_each_concept_where_it_first_appears():
    """Owner feedback 2026-11-05: say what the flow is for first, then walk the steps in order and explain a
    commit, a manifest, an ack and each y/N at the step that first needs it, in plain words. The glossary is
    reference at the end, not a wall up front."""
    text = README.read_text()
    # The opening: 3-5 plain sentences before the flow map: what GenieRails does, what you end up with,
    # and the journey's five parts in order. No glossary or "why" bullets before the flow map.
    opening = text[text.index("\n", text.index("# Ship")):text.index("\n## The whole flow\n")]
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", _prose(opening).strip()) if s.strip()]
    assert 3 <= len(sentences) <= 6, sentences
    assert "\n- " not in opening and "\n|" not in opening and "\n##" not in opening
    for fact in ("finds the sensitive columns", "into masks and access rules", "a dev and a prod env",
                 "each group sees only what it should", "proven before anyone new gets access"):
        assert fact in opening, fact
    journey = ["set up dev", "prove the rules in dev", "copy the proven rules to prod", "release prod",
               "keep it healthy"]
    assert [opening.index(part) for part in journey] == sorted(opening.index(part) for part in journey)
    # One sentence introduces the flow map.
    intro = text[text.index("\n## The whole flow\n") + len("\n## The whole flow\n"):text.index("```bash")]
    assert [line for line in intro.splitlines() if line.strip()] == [
        "The whole flow at a glance; the steps below walk through each line."]

    dev = text[text.index("\n## Dev\n"):text.index("\n## Prod\n")]
    prod = text[text.index("\n## Prod\n"):text.index("\n## Keep it healthy\n")]
    after_map = _prose(text[text.index("\n## Before you start\n"):text.index("\n## Reference\n")])

    # Commit: explained in Dev step 1, at setup's offer, before any other prose use of the word.
    setup = _step(dev, "1. Set up.")
    # REPO= in plain words (owner feedback 2026-11-11): what the URL is, why GenieRails pushes there, what
    # happens without it; no "origin" jargon in the prose (git's remote names live in DETAILS.md).
    for fact in ("`REPO=` (optional) is the URL of your own git repository, e.g. "
                 "`https://github.com/your-org/genierails-config`.",
                 "GenieRails saves your changes there (it asks first), so teammates can review them.",
                 "Without it, everything is saved only on this machine; you can add it later."):
        assert fact in setup, fact
    assert "origin" not in _prose(setup[:setup.index('<a id="commit-it"></a>')])
    for gone in ("makes this clone yours", "`origin` becomes your repo", "stays in local git"):
        assert gone not in text, gone
    offer = setup[setup.index('<a id="commit-it"></a>'):]
    for fact in ("**Then it offers to commit.** GenieRails previews", "`Commit these changes and push them to "
                 "origin/main? [y/N]`. Answer `y`.", "*What a commit is:* a saved snapshot of these files, with an ID.",
                 "- What you test in dev is exactly what reaches prod: prod releases only a commit that passed in dev.",
                 "- Teammates can review a change before it ships.",
                 "- Git keeps every version, so you can see what changed and undo it later."):
        assert fact in offer, fact
    before = after_map[:after_map.index("*What a commit is:*")]
    assert re.findall(r"commit", before, re.I) == ["commit"] and before.rstrip().endswith("Answer ."), before[-200:]
    # Classification review: why a column with no decision blocks access, with the design link, in Dev step 2.
    classify = _step(dev, "2. Review classification")
    for fact in ("*Why this review matters:*", "GenieRails builds the masks from these tags",
                 "Every tagged column the agent can reach needs a decision: a mask, or, if the detection is wrong, "
                 "an exclusion here.", "GenieRails opens no new access, so nothing can leak by accident.",
                 "](DETAILS.md#why-no-unmasked-sensitive-column)"):
        assert fact in classify, fact
    # Rehearse: why prove in dev first, and what the account y/N approves.
    rehearse = _step(dev, "4. Rehearse.")
    for fact in ("*Why prove it in dev first:* a mistake in dev is cheap.", "queries as each group to prove the masks",
                 "Prod releases only a commit that passed here.", "`Approve and apply this account manifest? [y/N]`",
                 "That plan is the account-wide setup all envs share, e.g. the masks' tag policy. `y` applies exactly "
                 "what it showed"):
        assert fact in rehearse, fact
    assert rehearse.index("*Why prove it in dev first:*") < rehearse.index("make rehearse ENV=dev")
    # Release: what a manifest is (the first prose use of the word), what y means, why rounds, access last.
    release = _step(prod, "4. Release.")
    for fact in ("*What a manifest is:* the exact list of changes this round will make, saved before anything "
                 "happens.", "- `y` applies exactly that manifest. If anything changed since it was planned, it refuses.",
                 "- Anything else applies nothing and stops.", "*Why several rounds:* a first release goes masks "
                 "first, access last", "Each round is proven before the next opens more.",
                 "Answer `y` each round until it prints `=== Release complete (prod) ===`."):
        assert fact in release, fact
    assert after_map.lower().index("manifest") == after_map.index("*What a manifest is:*") + len("*What a ")
    # Ack: what ack.txt is, with a one-line example, only where the release asks for it.
    # Owner and admin readers, in plain words (owner feedback 2026-11-11): why the first release stops, what an
    # ack is, what to do; the key format's detail lives in DETAILS.md.
    for fact in ("**The first release stops to ask about owners and admins.**",
                 "Owners of the prod catalog or tables, and admins, can always read the raw data.",
                 "GenieRails can't take that away, so it asks you to confirm you know about them: an ack, one line "
                 "in `envs/prod/ack.txt` saying \"I reviewed this access and accept it\".",
                 "Copy the `prod:reader:...` key from each printed line you accept",
                 "commit and push it yourself, then run `make release ENV=prod` again."):
        assert fact in release, fact
    assert release.index("can always read the raw data") < release.index("an ack, one line") \
        < release.index("Copy the `prod:reader:...` key") < release.index("```text")
    for gone in ("reader refusals", "<principal>", "GenieRails doesn't manage that access"):
        assert gone not in release, gone
    # Rehearsal reuse, in one plain sentence; the release step's goal says what a round is.
    assert "- It checks that this exact version already passed in dev. If not, it says `no passing rehearsal of " \
        "<commit>`: run `make rehearse ENV=dev` first." in release
    assert "**4. Release.** Apply the rules to prod in a few small steps (rounds); you approve each one." in release
    for gone in ("never rehearses dev", "reuses dev's rehearsal", "one approved round at a time"):
        assert gone not in text, gone
    assert "```text\n# reviewed: owner group of prod_finance\n" \
        "prod:reader:prod_finance.payments.transactions:finance_admins\n```" in release
    assert "never a `y/N`" in release
    first_ack = re.search(r"\back\b", after_map).start()
    assert first_ack == after_map.index("an ack, one line in") + len("an "), after_map[first_ack - 100:first_ack]
    # Each concept is explained once; later mentions link back.
    for once in ("*What a commit is:*", "*What a manifest is:*", "*Why several rounds:*", "*Why prove it in dev "
                 "first:*", "*Why this review matters:*", "saved snapshot", "the exact list of changes"):
        assert text.count(once) <= 2 and text[:text.index("\n## Reference\n")].count(once) == 1, once
    assert "Do the same review as [dev step 2](#dev-classify), on the prod catalog." in _step(prod, "3. Review")
    # Plain words: no jargon walls, short sentences.
    for word in ("provenance", "footprint", "fingerprint", "four-eyes", "IdP", "staged"):
        assert word.lower() not in _prose(text).lower(), word
    principals = [m.start() for m in re.finditer(r"principal", _prose(text))]
    assert principals and all(_prose(text)[p - 8:p] == "service " for p in principals)
    assert "service principal (a non-human account that runs the deploys" in text
    assert text.index("service principal (a non-human account") <= text.index("service principal")
    for sentence in re.split(r"(?<=[.!?])\s+|\n", _prose(text)):
        assert len(sentence.split()) <= 30, sentence

    # The reference closes the page: the ask table, then a small glossary.
    reference = text[text.index("\n## Reference\n"):]
    table = reference[reference.index("### What each command asks"):reference.index("### Words used on this page")]
    assert "| Command | What it asks | What your `y` approves |" in table
    rows = [line for line in table.splitlines() if line.startswith("| `make ")]
    assert [row.split(" | ")[0][2:] for row in rows] == [
        "`make setup ENV=dev REPO=<url>`", "`make generate ENV=dev`", "`make rehearse ENV=dev`",
        "`make setup ENV=prod`", "`make promote-to ENV=prod`", "`make release ENV=prod`"]
    for line in rows:
        cells = [c.strip() for c in line.strip().strip("|").split(" | ")]
        assert len(cells) == 3 and ("[y/N]" in cells[1] or cells[1] == "The same commit question"), line
        assert cells[2] not in ("", "—"), line
    assert "e.g. the masks' tag policy" in table and "exactly that round's prod changes" in table
    assert "`evidence`, `maintain`, `next` and `status` ask nothing." in table
    glossary = reference[reference.index("### Words used on this page"):]
    rows = [line for line in glossary.splitlines() if line.startswith("| **")]
    terms = [re.match(r"\| \*\*([^*]+)\*\*", row).group(1) for row in rows]
    assert terms == ["env", "tier", "classification tag", "treatment / mask", "commit", "rehearse", "manifest",
                     "ack"], terms
    for row in rows:
        assert len(row.split()) <= 40, row
    assert "| **commit** | A saved snapshot of your files, with an ID ([dev step 1](#commit-it)) |" in glossary
    assert "| **manifest** | The exact list of changes a step will make, saved before anything happens" in glossary
    assert "](REFERENCE.md#glossary)" in glossary and "\n## Glossary\n" in (WALKTHROUGH / "REFERENCE.md").read_text()
    # One plain line on why the tier order matters, where you gather the groups.
    assert "e.g. `payments_ops` sees raw values, `viewers` masked ones. Order matters" in text


def test_details_explains_why_no_class_is_raw_for_everyone():
    """Owner decision 2026-11-05: no 'raw for everyone' switch; the reason and today's raw paths are documented."""
    readme, details = README.read_text(), DETAILS.read_text()
    classify = readme[readme.index("**2. Review classification"):readme.index("**3. Generate.**")]
    assert "](DETAILS.md#why-no-unmasked-sensitive-column)" in classify
    start = details.index("\n## Why GenieRails won't open access to an unmasked sensitive column\n")
    section = details[start:details.index("\n## ", start + 1)]
    for fact in ("explicit, reviewed decision before anyone gets access", "can't leak by accident",
                 "stays masked", "live in git", "deliberately no switch that makes a class raw for everyone",
                 "exclude the detection, or turn off auto-tagging for that class", "Everyone who can read the table",
                 "not in git", "`coverage_acknowledged_columns`", "`access_tier_groups[0]`",
                 "Every classified column it can read", "`raw_exempt_principals`", "the everyone group is refused",
                 '`column_overrides = { "cat.sch.tbl.col" = { partial = "raw" } }`', "The middle tiers",
                 "`make release-plan` refuses them until a reviewed `ack.txt` line", "Never-raw classes",
                 "(CVV), stay masked for everyone in every case"):
        assert fact in section, fact
    rows = [line for line in section.splitlines() if line.startswith("| ") and not line.startswith("| Way")]
    assert len(rows) == 4, rows
    # Tier 1 and raw_exempt_principals see raw on every classified/masked column EXCEPT never-raw ones: the
    # cells themselves say so, naming every class the mask library never shows raw.
    never_raw = "except never-raw ones (CVV, card PIN, card track data, secrets)"
    cells = {row.split(" | ")[0]: [c.strip() for c in row.strip().strip("|").split(" | ")] for row in rows}
    tier1 = next(c for k, c in cells.items() if "`access_tier_groups[0]`" in k)
    exempt = next(c for k, c in cells.items() if k == "| `raw_exempt_principals`")
    assert tier1[2] == f"Every classified column it can read, {never_raw}", tier1
    assert exempt[2] == f"Every masked column, {never_raw}", exempt
    library = json.loads((ROOT / "shared" / "mask_library.json").read_text())
    assert library["never_raw_classes"] == ["card_security_code", "card_pin", "card_track_data", "secret"]
    # Only options that exist: tier_access_overrides is refused at generate, and no raw treatment exists.
    assert "tier_access_overrides" not in section and "treatment_versions" not in section
    # ...and the facts it cites hold in the code.
    shared = ROOT / "shared"
    assert "everyone would see raw values on every masked" in (shared / "deterministic_governance.py").read_text()
    assert '"card_security_code"' in (shared / "mask_library.json").read_text().split("never_raw_classes", 1)[1][:80]
    assert "tier_access_overrides are deferred" in (shared / "governance_policies.py").read_text()


# The 8-command block at the top, exactly. Each comment says one plain thing the command does for you; the
# human steps between commands are "then:" lines pointing at their step. Commit offers, y/N prompts, the
# first-time account approval and prod's keys live in the steps and the ask table, not in the comments.
FLOW_BLOCK = [
    "# Dev: build the rules and prove they work",
    "make setup ENV=dev REPO=<your-repo-url>   # 1. create dev's config files (optional REPO = URL of your own git repo, e.g. on GitHub, for team review)",
    "#   then: fill in dev's env and agent files                (Dev step 1)",
    "#   then: turn on Data classification in Catalog Explorer  (Dev step 2)",
    "make generate ENV=dev       # 2. draft mask and access rules from the classification tags",
    "make rehearse ENV=dev       # 3. apply them to dev and prove the masks hide the data",
    "",
    "# Prod: copy what dev proved, then release it (same for stg or any env)",
    "make setup ENV=prod         # 4. create prod's config files",
    "#   then: fill in prod's env file                          (Prod step 1)",
    "make promote-to ENV=prod    # 5. copy dev's tested rules into prod's config",
    "#   then: review classification on the prod catalog        (Prod step 3)",
    "make release ENV=prod       # 6. apply them to prod in a few small steps, showing you each step's changes to approve",
    "make evidence ENV=prod      # 7. save a record of what is live (for audit)",
    "make maintain ENV=prod      # 8. run on a schedule to mask columns that get tagged later",
    "",
    "# Stuck? `make next ENV=<env>` prints the next command.",
]
FLOW_AFTER = ("Some commands ask `y/N`, only at a terminal ([CI / scripted](#ci--scripted)): e.g. before a commit "
              "and push or a prod release round. [The table at the end](#what-each-command-asks) lists each question.")
# The crammed comments the block used to carry: none may come back.
OLD_FLOW_COMMENTS = [
    "creates dev's files (REPO= optional); offers to commit (y)",
    "fill in the env + agent files setup copied from the templates",
    "Catalog Explorer: turn on Data classification and review what it found",
    "drafts masks and access rules from the tags; offers to commit + push (y)",
    "checks, applies dev, proves the masks work; first time: asks y/N about account setup",
    "then set deployer_principal_id, catalog_map, group_map",
    "copies dev's proven rules to prod; offers to commit + push (y)",
    "Catalog Explorer: the same review on the prod catalog",
    "applies prod in rounds; shows each round's plan, asks y/N; ends at \"Release complete\"",
    "writes a record of what is live",
    "on a schedule, on the deployment machine: masks columns tagged later",
    "# lost? make next ENV=<env>",
    "(REPO= is optional)",
    "one approved round at a time",
]


def _flow_block(text: str) -> list[str]:
    flow = text[text.index("\n## The whole flow\n"):text.index("\n## Before you start\n")]
    start = flow.index("```bash\n") + len("```bash\n")
    return flow[start:flow.index("\n```", start)].splitlines()


def test_the_flow_block_says_one_plain_thing_per_command():
    text = README.read_text()
    block = _flow_block(text)
    assert block == FLOW_BLOCK
    # The 8 commands, in order, numbered 1-8; nothing else runs in the block.
    commands = [line for line in block if line and not line.startswith("#")]
    assert [line.split("#")[0].strip() for line in commands] == [
        "make setup ENV=dev REPO=<your-repo-url>", "make generate ENV=dev", "make rehearse ENV=dev",
        "make setup ENV=prod", "make promote-to ENV=prod", "make release ENV=prod", "make evidence ENV=prod",
        "make maintain ENV=prod"]
    assert [re.search(r"# (\d)\. ", line).group(1) for line in commands] == [str(n) for n in range(1, 9)]
    # One idea per comment: no lists of side effects, no prompts, no internal keys before the reader meets them.
    for line in block:
        comment = line.split("#", 1)[1] if "#" in line else ""
        for crammed in (";", "y/N", "(y)", "offers", "commit", "push", "deployer_principal_id", "catalog_map",
                        "group_map", "acl_groups", "manifest", "account"):
            assert crammed not in comment, (crammed, line)
    # Human steps are "then:" lines naming their step, never a command.
    humans = [line for line in block if line.startswith("#   ")]
    assert humans and all(line.startswith("#   then: ") and re.search(r"\((Dev|Prod) step \d\)$", line)
                          for line in humans)
    # Owner feedback 2026-11-11: the REPO comment says what REPO is and what it's for; the release comment says,
    # in plain words, that it goes in small steps you approve.
    repo = block[1].split("#", 1)[1]
    for fact in ("optional", "URL of your own git repo", "GitHub", "team review"):
        assert fact in repo, fact
    release = next(line for line in block if line.startswith("make release ENV=prod")).split("#", 1)[1]
    for fact in ("small steps", "each step's changes", "approve"):
        assert fact in release, fact
    for old in OLD_FLOW_COMMENTS:
        assert old not in "\n".join(block), old
    # One sentence introduces the block; one says where the y/N questions are.
    flow = text[text.index("\n## The whole flow\n"):text.index("\n## Before you start\n")]
    after = flow[flow.index("\n```\n", flow.index("```bash")) + len("\n```\n"):]
    assert [line for line in after.splitlines() if line.strip() and not line.startswith("<a id=")] == [FLOW_AFTER]


def test_the_facts_moved_out_of_the_flow_comments_live_in_the_steps_and_ask_table():
    text = README.read_text()
    dev = text[text.index("\n## Dev\n"):text.index("\n## Prod\n")]
    prod = text[text.index("\n## Prod\n"):text.index("\n## Keep it healthy\n")]
    healthy = text[text.index("\n## Keep it healthy\n"):text.index("\n## Later changes\n")]
    asks = text[text.index("### What each command asks"):text.index('<a id="glossary"></a>')]
    rows = {line.split(" | ")[0].strip("| `"): line for line in asks.splitlines() if line.startswith("| `make ")}
    moved = [
        # setup ENV=dev: REPO= is optional, the templates it copies, its commit offer.
        (_step(dev, "1. Set up."), ["`REPO=` (optional) is the URL of your own git repository",
                                    "Without it, everything is saved only on this machine",
                                    "Setup copies this walkthrough's templates into dev",
                                    "**Then it offers to commit.**", "Answer `y`."]),
        (rows["make setup ENV=dev REPO=<url>"], ["`Commit these changes and push them to origin/main? [y/N]`"]),
        # Dev step 2: turn on Data classification and review what it found.
        (_step(dev, "2. Review classification"), ["**Data classification**: turn it on", "review the detections"]),
        # generate: its commit and push offer.
        (_step(dev, "3. Generate."), ["It then offers to commit and push `.gitignore` and `envs/dev/`. Answer `y`."]),
        (rows["make generate ENV=dev"], ["The same commit question"]),
        # rehearse: runs make check first; first time, asks y/N about the account setup.
        (_step(dev, "4. Rehearse."), ["It first runs `make check ENV=dev`.",
                                      "A first deploy shows the shared account plan and asks `Approve and apply this "
                                      "account manifest? [y/N]`."]),
        (rows["make rehearse ENV=dev"], ["`Approve and apply this account manifest? [y/N]`, only when the account plan"]),
        # setup ENV=prod: which keys to set in prod's env file.
        (_step(prod, "1. Set up."), ["replace the `deployer_principal_id` placeholder and the seeded `catalog_map` line",
                                     "group_map   = {"]),
        (rows["make setup ENV=prod"], ["The same commit question"]),
        # promote-to: its commit and push offer.
        (_step(prod, "2. Copy the proven rules."), ["It then offers to commit and push `.gitignore`, `envs/prod/` and "
                                                    "`envs/account/`: answer `y`."]),
        (rows["make promote-to ENV=prod"], ["The same commit question"]),
        # Prod step 3: the same classification review on the prod catalog.
        (_step(prod, "3. Review classification"), ["Do the same review as [dev step 2](#dev-classify), on the prod catalog."]),
        # release: shows each round's manifest, asks y/N, ends at Release complete; the account question first.
        (_step(prod, "4. Release."), ["Each round shows you its manifest and asks `Approve and release manifest <id> ...? [y/N]`.",
                                      "Answer `y` each round until it prints `=== Release complete (prod) ===`.",
                                      "Before round 1, if the account plan has changes, it asks the same account question"]),
        (rows["make release ENV=prod"], ["`Approve and apply this account manifest? [y/N]` first",
                                         "once per round"]),
        # evidence: writes a record of what is live.
        (_step(prod, "5. Record evidence."), ["Write a record of what is live in prod, for auditors."]),
        # maintain: on a schedule, on the deployment machine, masks columns tagged later.
        (healthy, ["`maintain` catches columns tagged after the release and masks them.",
                   "Schedule it (e.g. with cron) on the [deployment machine](../../docs/deployment-machine.md)."]),
    ]
    for where, facts in moved:
        for fact in facts:
            assert fact in where, fact
    # The commands that ask nothing, and next.
    assert "`evidence`, `maintain`, `next` and `status` ask nothing." in asks
