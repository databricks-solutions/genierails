"""Canonical masking function registry.

Maps LLM-generated function name variations to canonical names.
Similar to tag_vocabulary.py but for masking/filter functions.

Usage:
    from function_registry import FUNCTION_REGISTRY

    canonical = FUNCTION_REGISTRY.canonical_name("mask_card_last4")
    # -> "mask_credit_card_last4"

    canonical = FUNCTION_REGISTRY.canonical_name("filter_aml_clearance")
    # -> "filter_aml_compliance"

    all_names = FUNCTION_REGISTRY.all_canonical_names()
"""

from __future__ import annotations

import json
from pathlib import Path


class FunctionRegistry:
    """Registry of canonical masking function names with alias resolution."""

    def __init__(self, data: dict) -> None:
        self._functions: dict[str, dict] = data.get("functions", {})
        # Build reverse lookup: alias -> canonical name
        self._alias_map: dict[str, str] = {}
        for canonical, info in self._functions.items():
            # The canonical name maps to itself
            self._alias_map[canonical] = canonical
            for alias in info.get("aliases", []):
                self._alias_map[alias] = canonical

    def canonical_name(self, name: str) -> str:
        """Return the canonical function name for an alias, or the input if not found."""
        return self._alias_map.get(name, name)

    def is_known(self, name: str) -> bool:
        """Return True if the name (or an alias) is in the registry."""
        return name in self._alias_map

    def all_canonical_names(self) -> set[str]:
        """Return all canonical function names."""
        return set(self._functions.keys())

    def category(self, name: str) -> str:
        """Return the category of a function (pii, financial, health, etc.)."""
        canonical = self.canonical_name(name)
        info = self._functions.get(canonical, {})
        return info.get("category", "unknown")

    def signature(self, name: str) -> str:
        """Return the function signature."""
        canonical = self.canonical_name(name)
        info = self._functions.get(canonical, {})
        return info.get("signature", "")

    def normalize_sql(self, sql_text: str) -> tuple[str, int]:
        """Normalize function names in SQL CREATE OR REPLACE FUNCTION statements.

        Returns (normalized_sql, count_of_renames).
        """
        import re
        count = 0
        for alias, canonical in self._alias_map.items():
            if alias == canonical:
                continue
            # Match CREATE OR REPLACE FUNCTION alias_name(
            pattern = re.compile(
                r'(CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\S+\.)*)'
                + re.escape(alias)
                + r'(\s*\()',
                re.IGNORECASE,
            )
            new_sql, n = pattern.subn(rf'\g<1>{canonical}\2', sql_text)
            if n > 0:
                sql_text = new_sql
                count += n
        return sql_text, count

    def normalize_hcl(self, hcl_text: str) -> tuple[str, int]:
        """Normalize function names in HCL tfvars (function_name = "...").

        Returns (normalized_hcl, count_of_renames).
        """
        count = 0
        for alias, canonical in self._alias_map.items():
            if alias == canonical:
                continue
            old = f'"{alias}"'
            new = f'"{canonical}"'
            if old in hcl_text:
                hcl_text = hcl_text.replace(old, new)
                count += 1
        return hcl_text, count

    @classmethod
    def load_default(cls) -> "FunctionRegistry":
        """Load the default registry from function_registry.json."""
        registry_path = Path(__file__).parent / "function_registry.json"
        with open(registry_path) as f:
            data = json.load(f)
        return cls(data)


# Singleton instance
FUNCTION_REGISTRY = FunctionRegistry.load_default()
