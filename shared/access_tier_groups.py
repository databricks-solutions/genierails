"""User-owned ``access_tier_groups`` setting in a workspace env.auto.tfvars.

The setting is the consume-mode group->tier mapping: existing IdP-synced group
names, ordered most- to least-privileged. ``make generate`` reads it so the
mapping is typed once instead of repeated as ``GENERATE_ARGS='--groups ...'``,
and ``make promote`` carries it to the destination env so prod uses the same
tiers. Terraform declares it (default ``[]``) only so the shared file loads
without an undeclared-variable warning; no resource reads it.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

from genie_space_placeholder import _display_path as display_path

SETTING = "access_tier_groups"

_HEREDOC = re.compile(r"<<-?([A-Za-z_][A-Za-z0-9_]*)[ \t]*\n")
_IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
_OPEN = {"[": "]", "{": "}", "(": ")"}


def parse_groups_arg(value: str) -> list[str]:
    """Split a ``--groups 'a,b,c'`` value into ordered, non-empty names."""
    return [group.strip() for group in value.split(",") if group.strip()]


def persisted_access_tier_groups(config: dict, path: Path) -> list[str]:
    """Return the persisted ordered groups ([] when unset), or raise ValueError."""
    value = config.get(SETTING)
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(group, str) and group.strip() for group in value
    ):
        raise ValueError(
            f"{SETTING} in {display_path(path)} must be a list of group names, "
            'most to least privileged (e.g. ["payments_ops", "viewers"])'
        )
    groups = [group.strip() for group in value]
    duplicates = sorted({group for group in groups if groups.count(group) > 1})
    if duplicates:
        raise ValueError(
            f"{SETTING} in {display_path(path)} lists {', '.join(duplicates)} "
            "more than once; each access tier needs its own group"
        )
    return groups


def _hcl_string(value: str) -> str:
    # JSON escapes are valid HCL; also neutralise template sequences.
    return json.dumps(value).replace("${", "$${").replace("%{", "%%{")


def render(groups: list[str]) -> str:
    return f"{SETTING} = [{', '.join(_hcl_string(group) for group in groups)}]"


def _code_mask(text: str) -> tuple[list[bool], set[int]]:
    """Mark code characters (not in strings, comments, heredocs) and comment starts."""
    mask = [True] * len(text)
    comments: set[int] = set()
    i, n = 0, len(text)

    def blank(start: int, end: int) -> None:
        for k in range(start, min(end, n)):
            mask[k] = False

    while i < n:
        ch = text[i]
        if ch == "#" or text.startswith("//", i):
            comments.add(i)
            end = text.find("\n", i)
            end = n if end == -1 else end
            blank(i, end)
            i = end
        elif text.startswith("/*", i):
            comments.add(i)
            end = text.find("*/", i + 2)
            end = n if end == -1 else end + 2
            blank(i, end)
            i = end
        elif ch == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            blank(i, j + 1)
            i = j + 1
        elif ch == "<" and (match := _HEREDOC.match(text, i)):
            closer = re.compile(rf"^[ \t]*{match.group(1)}[ \t]*$", re.MULTILINE)
            found = closer.search(text, match.end())
            end = n if found is None else found.end()
            blank(i, end)
            i = end
        else:
            i += 1
    return mask, comments


def _assignment_spans(text: str) -> list[tuple[int, int]]:
    """Return (start, end) of every top-level ``access_tier_groups = <value>``."""
    mask, comments = _code_mask(text)
    n = len(text)
    spans: list[tuple[int, int]] = []
    depth = 0
    i = 0
    while i < n:
        if not mask[i]:
            i += 1
            continue
        ch = text[i]
        if ch in _OPEN:
            depth += 1
        elif ch in _OPEN.values():
            depth -= 1
        elif (
            depth == 0
            and text.startswith(SETTING, i)
            and (i == 0 or text[i - 1] not in _IDENT_CHARS)
            and (i + len(SETTING) >= n or text[i + len(SETTING)] not in _IDENT_CHARS)
        ):
            j = i + len(SETTING)
            while j < n and text[j] in " \t":
                j += 1
            if j < n and text[j] == "=" and text[j + 1 : j + 2] != "=":
                j += 1
                while j < n and text[j] in " \t":
                    j += 1
                end = _value_end(text, mask, comments, j)
                spans.append((i, end))
                i = end
                continue
        i += 1
    return spans


def _value_end(text: str, mask: list[bool], comments: set[int], start: int) -> int:
    """End offset of the HCL value starting at ``start``."""
    n = len(text)
    if start < n and mask[start] and text[start] in _OPEN:
        depth = 0
        for k in range(start, n):
            if not mask[k]:
                continue
            if text[k] in _OPEN:
                depth += 1
            elif text[k] in _OPEN.values():
                depth -= 1
                if depth == 0:
                    return k + 1
        raise ValueError(f"unterminated {SETTING} value")
    # Scalar (null, a string, ...): up to the end of the line or a comment.
    end = start
    while end < n and text[end] != "\n" and end not in comments:
        end += 1
    return start + len(text[start:end].rstrip())


def persist_access_tier_groups(path: Path, groups: list[str]) -> None:
    """Write ``groups`` into an env.auto.tfvars whose setting is unset or [].

    Replaces an existing top-level assignment in place, otherwise appends one.
    Strings, comments, heredocs and every other setting are left untouched.
    Idempotent for the same groups; refuses (ValueError) to overwrite a
    different non-empty saved value.
    """
    import hcl2

    target = Path(path).resolve()
    text = target.read_text()
    before = hcl2.loads(text)
    saved = persisted_access_tier_groups(before, target)
    if saved == list(groups):
        return
    if saved:
        raise ValueError(
            f"{SETTING} in {display_path(target)} is already set; "
            "edit it there instead of overwriting it"
        )
    line = render(groups)
    spans = _assignment_spans(text)
    if len(spans) > 1:
        raise ValueError(f"{SETTING} is assigned more than once in {display_path(target)}")
    if spans:
        start, end = spans[0]
        current = text[start:end]
        inner = current[current.index("[") + 1 : -1] if current.endswith("]") else ""
        if inner.strip():
            # Keep comments written inside the empty list; append the items.
            items = ", ".join(_hcl_string(group) for group in groups)
            line = f"{current[:current.index('[') + 1]}{inner.rstrip()}\n  {items},\n]"
        updated = text[:start] + line + text[end:]
    else:
        updated = text + ("" if text.endswith("\n") or not text else "\n")
        updated += (
            "\n# Access-tier groups, most to least privileged (saved by make generate).\n"
            f"{line}\n"
        )
    after = hcl2.loads(updated)
    before[SETTING] = list(groups)
    if after != before:
        raise ValueError(
            f"could not safely update {SETTING} in {display_path(target)}; set it by hand"
        )
    with tempfile.NamedTemporaryFile(
        mode="w", dir=target.parent, prefix=f".{target.name}.", delete=False
    ) as tmp:
        tmp.write(updated)
        tmp_path = Path(tmp.name)
    os.chmod(tmp_path, target.stat().st_mode & 0o777)
    os.replace(tmp_path, target)


def promoted_lines(config: dict, source_path: Path) -> list[str]:
    """Destination env.auto.tfvars lines carrying the source's tiers verbatim.

    Groups are account-level names, so no catalog remap applies. Returns []
    when the source has no persisted setting.
    """
    groups = persisted_access_tier_groups(config, source_path)
    if not groups:
        return []
    return [
        "",
        "# Same access tiers as the promoted rules (most to least privileged).",
        render(groups),
    ]
