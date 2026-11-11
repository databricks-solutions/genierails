"""The root README is a landing page: what GenieRails is, why you'd use it, where to start.

Internals (release git checks, the coverage check's mechanics, bootstrap-sp's secret handling)
live in REFERENCE.md, shared/docs/ and the walkthrough's DETAILS.md, not here. These tests keep
the page short, in plain words, in its fixed section order, and with every link resolving.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"

SECTIONS = ["## Why use it", "## How it works", "## Get started", "## Other ways to use it", "## Docs"]
MAX_WORDS = 450
# Internal terms a first-time reader shouldn't meet on the landing page.
JARGON = [r"provenance", r"manifest", r"CAN_RUN", r"option-A", r"coverage[ -]gate", r"coverage check",
          r"\bSP\b", r"origin/main", r"bootstrap", r"release-plan", r"receipt", r"tfvars", r"\bABAC\b",
          r"deterministic", r"\backs?\b", r"champion", r"GENIERAILS_", r"fail(?:s)? closed"]
_LINK = re.compile(r"\]\(([^)\s]+)\)")


def _text() -> str:
    return README.read_text()


def _prose(text: str) -> str:
    """The page without its HTML logo block and fenced code."""
    text = re.sub(r"<p[^>]*>.*?</p>", "", text, flags=re.S)
    return re.sub(r"```.*?```", "", text, flags=re.S)


def _section(text: str, heading: str) -> str:
    start = text.index(f"\n{heading}\n")
    nxt = text.find("\n## ", start + 1)
    return text[start:] if nxt == -1 else text[start:nxt]


def _slug(heading: str) -> str:
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    text = path.read_text()
    return set(re.findall(r'<a id="([^"]+)"></a>', text)) | {
        _slug(h) for h in re.findall(r"^#+ (.+)$", text, re.M)}


def test_readme_keeps_the_logo_and_title_then_says_what_it_is():
    text = _text()
    assert '<img src="shared/docs/genierails-logo.png" alt="GenieRails"' in text
    assert (ROOT / "shared/docs/genierails-logo.png").exists()
    intro = text[text.index("\n# GenieRails\n"):text.index(f"\n{SECTIONS[0]}\n")]
    assert "GenieRails lets you give business users a Genie agent over your data without exposing sensitive columns." in intro
    assert "Data Classification" in intro and "dev" in intro and "prod" in intro
    assert "You don't write any Terraform." in intro


def test_readme_sections_are_what_why_how_start_other_docs_in_order():
    text = _text()
    headings = re.findall(r"^## .+$", text, re.M)
    assert headings == SECTIONS, headings
    assert re.findall(r"^# .+$", _prose(text), re.M) == ["# GenieRails"]


def test_readme_is_short():
    words = len(_prose(_text()).split()) + sum(len(b.split()) for b in re.findall(r"```.*?```", _text(), re.S))
    assert words <= MAX_WORDS, words
    # Why: 3 to 5 bullets. How: at most 4 numbered steps.
    why = [line for line in _section(_text(), "## Why use it").splitlines() if line.startswith("- ")]
    assert 3 <= len(why) <= 5, why
    how = re.findall(r"^\d+\. ", _section(_text(), "## How it works"), re.M)
    assert 1 <= len(how) <= 4, how


def test_readme_has_no_internal_jargon():
    prose = _prose(_text())
    found = [term for term in JARGON if re.search(term, prose, re.IGNORECASE if term[0] != "\\" else 0)]
    assert not found, found


def test_get_started_points_to_the_walkthrough_and_make_next():
    start = _section(_text(), "## Get started")
    assert "**[Dev-to-Prod walkthrough](shared/examples/dev_to_prod/)**" in start
    assert "30 minutes" in start
    assert "Stuck? Run `make next ENV=<env>`." in start
    steps = ["make setup ENV=dev", "make generate ENV=dev", "make rehearse ENV=dev", "make setup ENV=prod",
             "make promote-to ENV=prod", "make release ENV=prod"]
    positions = [start.index(step) for step in steps]
    assert positions == sorted(positions), steps


def test_make_commands_appear_only_in_get_started():
    """What/why/how text describes outcomes; the command teaser lives only in Get started."""
    text = _text()
    start = _section(text, "## Get started")
    rest = text.replace(start, "\n")
    found = re.findall(r"\bmake [A-Za-z][\w-]*", rest)
    assert not found, found
    assert "`make` commands" not in rest
    assert "make evidence" not in rest


def test_readme_says_it_controls_who_can_use_the_genie_agent():
    text = _text()
    intro = text[text.index("\n# GenieRails\n"):text.index(f"\n{SECTIONS[0]}\n")]
    # Access opens only after the masks are applied AND verified (the staged release runs verify-access
    # before its grant phase, and Genie access comes last), so say "proven", not merely "in place".
    assert "It also controls who can use each Genie agent, and only opens that access once the masks are proven to work." in intro
    assert not re.search(r"masks (?:are|were) (?:in place|applied|there)\b", _prose(text)), "say the masks are proven to work"
    why = _section(text, "## Why use it")
    assert ("Nobody gets new access to an agent or its data until every sensitive column Data Classification "
            "finds in its tables has a mask.") in why
    assert "Prod only takes changes that were committed and tested" in why
    how = _section(text, "## How it works")
    assert "who can use each Genie agent, using your existing groups" in how
    assert "Masks go on before anyone gets access to the tables or the agent." in how
    assert "every sensitive column it can reach" not in text


def test_other_ways_and_docs_link_the_playbooks_and_reference():
    text = _text()
    other = _section(text, "## Other ways to use it")
    for link in ("shared/docs/playbooks/governance.md#govern-tables-only", "shared/docs/playbooks/one-team.md",
                 "shared/docs/pipelines.md", "shared/docs/deployment-machine.md"):
        assert f"]({link})" in other, link
    docs = _section(text, "## Docs")
    assert "](shared/docs/)" in docs and "](REFERENCE.md)" in docs


def test_every_readme_link_resolves():
    links = _LINK.findall(_text())
    assert links
    for link in links:
        if re.match(r"[a-z]+:", link):
            continue
        target, _, anchor = link.partition("#")
        path = (ROOT / target).resolve() if target else README
        assert path.exists(), link
        if anchor:
            assert anchor in _anchors(path), link


def test_links_into_the_root_readme_resolve():
    """Docs that link to a root README anchor still land on a heading."""
    known = _anchors(README)
    tracked = subprocess.run(["git", "ls-files", "*.md"], cwd=ROOT, text=True, capture_output=True, check=True)
    for path in tracked.stdout.split():
        if "CHANGELOG" in path:
            continue
        for target, anchor in re.findall(r"\]\(((?:\.\./)*)README\.md#([A-Za-z0-9_-]+)\)", (ROOT / path).read_text()):
            if (ROOT / path).parent.joinpath(target, "README.md").resolve() == README.resolve():
                assert anchor in known, (path, anchor)


def test_internal_details_left_the_landing_page_for_their_docs():
    """What the old landing page said about internals still lives in the reference docs."""
    reference = (ROOT / "REFERENCE.md").read_text()
    assert "option-A local-only repo with no remotes or only the canonical `genierails` upgrade remote" in reference
    assert "Anything ambiguous (extra remotes, non-upstream `genierails`, or URL rewrites" in reference
    assert "By default a newly minted OAuth secret is printed once; with `BOOTSTRAP_AUTH_ENV`" in reference
    assert "pass `SP_NAME=<its name>` for an existing SP, or it creates a second one" in reference
    assert "| `make next` | `ENV` | Read-only: print the one next command" in reference
    assert "| `make status` | `ENV`, `JSON=1` (machine-readable) | Read-only" in reference
    architecture = (ROOT / "shared/docs/architecture.md").read_text()
    assert ("Terraform refuses to plan new or wider business `SELECT` or Genie `CAN_RUN` without a recent passing "
            "coverage result") in architecture
    assert "masks and policies are created before any grant" in architecture
    machine = (ROOT / "shared/docs/deployment-machine.md").read_text()
    assert "only the canonical `genierails` upgrade remote" in machine
    index = (ROOT / "shared/docs/README.md").read_text()
    assert "](../../README.md#get-started)" in index
    assert "](deployment-machine.md)" in index and "](architecture.md)" in index
