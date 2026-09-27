#!/usr/bin/env python3
"""Preserve and adopt an existing catalog classification config, if present."""

import json
import sys
from pathlib import Path

import hcl2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def _load(path: Path) -> dict:
    with path.open() as handle:
        return hcl2.load(handle)


def _value(config: dict, key: str) -> str:
    value = config.get(key, "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def main() -> int:
    env_dir = Path(sys.argv[1]).resolve()
    config = _load(env_dir / "env.auto.tfvars")
    auth = _load(env_dir / "auth.auto.tfvars")

    tables = list(config.get("uc_tables") or [])
    for space in config.get("genie_spaces") or []:
        tables.extend(space.get("uc_tables") or [])
    catalogs = sorted({table.split(".")[0] for table in tables})

    client = WorkspaceClient(
        host=_value(auth, "databricks_workspace_host"),
        client_id=_value(auth, "databricks_client_id"),
        client_secret=_value(auth, "databricks_client_secret"),
    )
    existing: dict[str, list[str]] = {}
    for catalog in catalogs:
        name = f"catalogs/{catalog}/config"
        try:
            remote = client.data_classification.get_catalog_config(name)
        except NotFound:
            continue
        existing[catalog] = sorted(set((remote.included_schemas.names or [])))

    output = env_dir / "data_access" / "classification.auto.tfvars"
    lines = ["classification_existing_schemas = {"]
    for catalog, schemas in existing.items():
        rendered = ", ".join(json.dumps(schema) for schema in schemas)
        lines.append(f"  {json.dumps(catalog)} = [{rendered}]")
    lines.append("}")
    output.write_text("\n".join(lines) + "\n")

    for catalog in existing:
        print(catalog)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
