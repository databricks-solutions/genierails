"""Explicit marker for envs that follow the dev-to-prod walkthrough.

The walkthrough template (shared/examples/dev_to_prod/env.auto.tfvars.example)
starts with a header line containing MARKER, and ``make promote`` writes the
same header into the destination env.auto.tfvars. Only this marker selects the
walkthrough's next-step advice; every other env keeps the apply / apply-genie
advice. It is a comment, not a setting, so it adds no user-facing input.
"""

from __future__ import annotations

from pathlib import Path

MARKER = "GenieRails Dev-to-Prod Walkthrough"
PROMOTED_HEADER = f"# {MARKER} — promoted by make promote (keep this line)"


def follows_walkthrough(env_file: Path) -> bool:
    """True when env.auto.tfvars carries the walkthrough marker."""
    try:
        return any(
            line.lstrip().startswith("#") and MARKER in line
            for line in env_file.read_text().splitlines()
        )
    except OSError:
        return False
