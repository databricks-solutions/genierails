#!/usr/bin/env python3
"""Explain a plan whose only destroys replace terraform_data.masking_functions.

Every masking SQL change replaces terraform_data.masking_functions, so plan
and apply report "must be replaced" and "1 to destroy". That replacement only
re-runs CREATE OR REPLACE FUNCTION; it never drops live mask functions. Reads
a copy of Terraform's plan or apply output (terraform_layer.sh tees it; the
output itself is never filtered) and prints one note when every resource the
plan destroys is such a replacement. Never fails.

Usage: python3 masking_replace_note.py <terraform-output-file>
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

NOTE = ("Note: replacing terraform_data.masking_functions only re-runs CREATE OR REPLACE; "
        "it never drops live mask functions.")

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_ACTION = re.compile(
    r"^\s*# (\S+)(?: \(deposed object \S+\))? "
    r"(will be destroyed|must be replaced|is tainted, so must be replaced)\s*$"
)
_PLAN = re.compile(r"^Plan: \d+ to add, \d+ to change, (\d+) to destroy\.")
_MASKING = re.compile(r"(?:^|\.)terraform_data\.masking_functions(?:\[[^\]]*\])?$")


def needs_note(lines: list[str]) -> bool:
    """True when the plan destroys something and every destroy is a
    masking_functions replacement (and the plan summary agrees)."""
    actions: list[tuple[str, str]] = []
    destroy_count = None
    for raw in lines:
        line = _ANSI.sub("", raw).rstrip("\n")
        if match := _ACTION.match(line):
            actions.append((match.group(1), match.group(2)))
        elif match := _PLAN.match(line.strip()):
            destroy_count = int(match.group(1))
    if not actions or destroy_count != len(actions):
        return False
    return all(action.endswith("must be replaced") and _MASKING.search(address)
               for address, action in actions)


def main(argv: list[str]) -> int:
    try:
        lines = Path(argv[1]).read_text(errors="replace").splitlines()
    except (IndexError, OSError):
        return 0
    if needs_note(lines):
        print(NOTE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
