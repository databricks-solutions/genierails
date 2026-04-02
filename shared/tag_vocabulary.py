from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REGISTRY_PATH = SCRIPT_DIR / "tag_vocabulary_registry.json"
_TAG_VALUE_REF_RE = re.compile(
    r"hasTagValue\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)"
)
_TAG_KEY_REF_RE = re.compile(r"hasTag\(\s*'([^']+)'\s*\)")


@dataclass(frozen=True)
class FamilySpec:
    canonical_key: str
    description: str
    canonical_values: tuple[str, ...]
    key_aliases: tuple[str, ...]
    value_aliases: dict[str, str]


@dataclass(frozen=True)
class KeyMatch:
    family: str
    canonical_key: str
    key_root: str
    suffix: str


class TagVocabularyRegistry:
    def __init__(self, families: dict[str, FamilySpec]):
        self._families = families
        roots: list[tuple[str, str]] = []
        for family_name, spec in families.items():
            roots.append((spec.canonical_key, family_name))
            for alias in spec.key_aliases:
                roots.append((alias, family_name))
        self._roots = sorted(
            roots,
            key=lambda item: len(item[0]),
            reverse=True,
        )

    @classmethod
    def load_default(cls) -> "TagVocabularyRegistry":
        raw = json.loads(REGISTRY_PATH.read_text())
        families: dict[str, FamilySpec] = {}
        for family_name, spec in raw.get("families", {}).items():
            families[family_name] = FamilySpec(
                canonical_key=spec["canonical_key"],
                description=spec.get("description", ""),
                canonical_values=tuple(spec.get("canonical_values", [])),
                key_aliases=tuple(spec.get("key_aliases", [])),
                value_aliases=dict(spec.get("value_aliases", {})),
            )
        return cls(families)

    def _match_key(self, tag_key: str) -> KeyMatch | None:
        for root, family_name in self._roots:
            if tag_key == root:
                spec = self._families[family_name]
                return KeyMatch(
                    family=family_name,
                    canonical_key=spec.canonical_key,
                    key_root=root,
                    suffix="",
                )
            if tag_key.startswith(f"{root}_"):
                spec = self._families[family_name]
                return KeyMatch(
                    family=family_name,
                    canonical_key=spec.canonical_key,
                    key_root=root,
                    suffix=tag_key[len(root):],
                )
        return None

    def spec_for_key(self, tag_key: str) -> FamilySpec | None:
        match = self._match_key(tag_key)
        if not match:
            return None
        return self._families[match.family]

    def family_for_key(self, tag_key: str) -> str | None:
        match = self._match_key(tag_key)
        return match.family if match else None

    def is_governed_key(self, tag_key: str) -> bool:
        return self._match_key(tag_key) is not None

    def canonical_key(self, tag_key: str) -> str:
        match = self._match_key(tag_key)
        if not match:
            return tag_key
        return match.canonical_key + match.suffix

    def canonical_values_for_key(self, tag_key: str) -> set[str] | None:
        spec = self.spec_for_key(tag_key)
        if not spec:
            return None
        return set(spec.canonical_values)

    def canonical_value(self, tag_key: str, tag_value: str) -> str:
        spec = self.spec_for_key(tag_key)
        if not spec:
            return tag_value
        return spec.value_aliases.get(tag_value, tag_value)

    def is_allowed_value(self, tag_key: str, tag_value: str) -> bool | None:
        allowed = self.canonical_values_for_key(tag_key)
        if allowed is None:
            return None
        return self.canonical_value(tag_key, tag_value) in allowed

    def normalize_condition_refs(self, condition: str) -> tuple[str, int]:
        updates = 0

        def _replace_tag_value(match: re.Match[str]) -> str:
            nonlocal updates
            key, value = match.group(1), match.group(2)
            canonical_key = self.canonical_key(key)
            canonical_value = self.canonical_value(canonical_key, value)
            if canonical_key != key or canonical_value != value:
                updates += 1
            return f"hasTagValue('{canonical_key}', '{canonical_value}')"

        def _replace_tag(match: re.Match[str]) -> str:
            nonlocal updates
            key = match.group(1)
            canonical_key = self.canonical_key(key)
            if canonical_key != key:
                updates += 1
            return f"hasTag('{canonical_key}')"

        normalized = _TAG_VALUE_REF_RE.sub(_replace_tag_value, condition or "")
        normalized = _TAG_KEY_REF_RE.sub(_replace_tag, normalized)
        return normalized, updates

    def iter_condition_value_refs(self, condition: str) -> list[tuple[str, str]]:
        return _TAG_VALUE_REF_RE.findall(condition or "")

    def iter_condition_key_refs(self, condition: str) -> list[str]:
        return _TAG_KEY_REF_RE.findall(condition or "")

    def render_prompt_block(self) -> str:
        lines = [
            "### CANONICAL TAG VOCABULARY",
            "",
            (
                "Use these canonical tag families exactly whenever "
                "the data maps to them."
            ),
            (
                "Do not invent synonyms for these keys or values. "
                "If you need another tag family, define a new key "
                "explicitly and keep its values self-consistent."
            ),
            "",
        ]
        for family_name in sorted(self._families):
            spec = self._families[family_name]
            lines.append(f"- `{spec.canonical_key}`: {spec.description}")
            lines.append(
                "  Allowed canonical values: "
                + ", ".join(f"`{value}`" for value in spec.canonical_values)
            )
            if spec.key_aliases:
                lines.append(
                    "  Normalize key aliases to this key: "
                    + ", ".join(f"`{alias}`" for alias in spec.key_aliases)
                )
            if spec.value_aliases:
                aliases = [
                    f"`{alias}` -> `{canonical}`"
                    for alias, canonical in sorted(spec.value_aliases.items())
                    if alias != canonical
                ]
                if aliases:
                    lines.append(
                        "  Normalize value aliases to canonical values: "
                        + ", ".join(aliases)
                    )
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"


REGISTRY = TagVocabularyRegistry.load_default()
