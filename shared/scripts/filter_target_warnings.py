#!/usr/bin/env python3
"""Drop Terraform's known-benign ``-target`` warnings from piped output.

``make enable-classification`` applies only the classification resource with
``-target``, so Terraform always prints "Resource targeting is in effect" and
"Applied changes may be incomplete". Both are expected there and alarm
first-time users. A diagnostic box is dropped only when its COMPLETE content
is one of those two warnings (compared with colour codes, box glyphs and line
wrapping removed). Any other box, including one with an error interleaved
into it from merged stderr, is emitted verbatim, as is every line outside a
box and an unterminated box at EOF.

Usage: terraform ... 2>&1 | python3 -u filter_target_warnings.py
"""

from __future__ import annotations

import re
import sys
from typing import Iterable, Iterator

# Terraform 1.x text of the two warnings (shared/tests/fixtures holds a capture).
BENIGN_TARGET_WARNINGS = (
    "Warning: Resource targeting is in effect "
    "You are creating a plan with the -target option, which means that the "
    "result of this plan may not represent all of the changes requested by the "
    "current configuration. "
    "The -target option is not for routine use, and is provided only for "
    "exceptional situations such as recovering from errors or mistakes, or when "
    "Terraform specifically suggests to use it as part of an error message.",
    "Warning: Applied changes may be incomplete "
    "The plan was created with the -target option in effect, so some changes "
    "requested in the configuration may have been ignored and the output values "
    "may not be fully updated. Run the following command to verify that no other "
    "changes are pending: terraform plan "
    "Note that the -target option is not suitable for routine use, and is "
    "provided only for exceptional situations such as recovering from errors or "
    "mistakes, or when Terraform specifically suggests to use it as part of an "
    "error message.",
)

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_OPEN = "╷"
_CLOSE = "╵"
_BAR = "│"


def _plain(line: str) -> str:
    return _ANSI.sub("", line).strip()


def _is_benign(inner: list[str]) -> bool:
    """True only if every inner line is a box line and together they spell a known warning."""
    words: list[str] = []
    for line in inner:
        plain = _plain(line)
        if not plain.startswith(_BAR):
            return False  # e.g. an error line interleaved from stderr
        words.extend(plain[len(_BAR):].split())
    return " ".join(words) in BENIGN_TARGET_WARNINGS


def filter_lines(lines: Iterable[str]) -> Iterator[str]:
    """Yield ``lines`` minus the benign ``-target`` warning boxes."""
    block: list[str] | None = None
    for line in lines:
        plain = _plain(line)
        if block is None:
            if plain == _OPEN:
                block = [line]
            else:
                yield line
            continue
        block.append(line)
        if plain == _CLOSE:
            if not _is_benign(block[1:-1]):
                yield from block
            block = None
    if block:  # unterminated box at EOF: never swallow it
        yield from block


def main() -> int:
    for line in filter_lines(sys.stdin):
        sys.stdout.write(line)
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
