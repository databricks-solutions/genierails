#!/usr/bin/env python3
"""Exit successfully when workspace host and client ID are non-placeholder values."""

import sys
from pathlib import Path

import hcl2


def auth_configured(path: Path) -> bool:
    try:
        with path.open() as handle:
            config = hcl2.load(handle)
    except (OSError, ValueError):
        return False

    def configured(name: str) -> bool:
        value = str(config.get(name, "") or "").strip()
        normalized = value.lower().replace("-", "_").replace(" ", "_")
        placeholders = ("placeholder", "change_me", "changeme", "your_client_id",
                        "your_workspace_host")
        return bool(value) and "<" not in value and ">" not in value and not any(
            marker in normalized for marker in placeholders
        )

    return configured("databricks_client_id") and configured("databricks_workspace_host")


if __name__ == "__main__":
    raise SystemExit(0 if len(sys.argv) == 2 and auth_configured(Path(sys.argv[1])) else 1)
