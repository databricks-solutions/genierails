#!/usr/bin/env python3
"""Drop Terraform's known-benign ``-target`` warnings from piped output.

``make enable-classification`` applies only the classification resource with
``-target``, so Terraform always prints "Resource targeting is in effect" and
"Applied changes may be incomplete". Both are expected there and alarm
first-time users. This filter removes exactly those two diagnostic blocks and
passes every other line through unchanged, including all errors and any other
warning.

Usage: terraform ... 2>&1 | python3 -u filter_target_warnings.py
"""

from __future__ import annotations

import re
import sys
from typing import Iterable, Iterator

BENIGN_TARGET_WARNINGS = (
    "Warning: Resource targeting is in effect",
    "Warning: Applied changes may be incomplete",
)

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_OPEN = "╷"
_CLOSE = "╵"


def _plain(line: str) -> str:
    return _ANSI.sub("", line).strip()


def _is_benign(block: list[str]) -> bool:
    for line in block:
        text = _plain(line).lstrip("│").strip()
        if text:
            return text in BENIGN_TARGET_WARNINGS
    return False


def filter_lines(lines: Iterable[str]) -> Iterator[str]:
    """Yield ``lines`` minus the benign ``-target`` warning blocks."""
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
    if block:  # unterminated block: never swallow it
        yield from block


def main() -> int:
    for line in filter_lines(sys.stdin):
        sys.stdout.write(line)
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
