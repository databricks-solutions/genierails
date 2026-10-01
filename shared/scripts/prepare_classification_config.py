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

    if not config.get("enable_classification", False):
        output = env_dir / "data_access" / "classification.auto.tfvars"
        output.write_text(
            "classification_existing_schemas = {}\n"
            "classification_all_schemas = []\n"
        )
        return 0

    tables = list(config.get("uc_tables") or [])
    for space in config.get("genie_spaces") or []:
        tables.extend(space.get("uc_tables") or [])
    discovered_path = env_dir / "data_access" / "discovered_uc_tables.auto.tfvars"
    if discovered_path.exists():
        discovered = _load(discovered_path)
        tables.extend(discovered.get("discovered_uc_tables") or [])
    uc_catalog = _value(config, "uc_catalog")
    full_tables = [
        table if len(table.split(".")) >= 3 else f"{uc_catalog}.{table}"
        for table in tables
        if len(table.split(".")) >= 3 or uc_catalog
    ]
    catalogs = sorted({table.split(".")[0] for table in full_tables})
    desired_schemas = {
        catalog: sorted(
            {
                table.split(".")[1]
                for table in full_tables
                if table.split(".")[0] == catalog
            }
        )
        for catalog in catalogs
    }

    workspace_id = _value(auth, "databricks_workspace_id")
    routing_headers = (
        {"X-Databricks-Org-Id": workspace_id}
        if workspace_id
        else None
    )
    client = WorkspaceClient(
        host=_value(auth, "databricks_workspace_host"),
        client_id=_value(auth, "databricks_client_id"),
        client_secret=_value(auth, "databricks_client_secret"),
        custom_headers=routing_headers,
    )
    existing: dict[str, list[str]] = {}
    all_schemas: set[str] = set()
    usage_policy_id = _value(auth, "serverless_usage_policy_id")
    for catalog in catalogs:
        name = f"catalogs/{catalog}/config"
        try:
            remote = client.data_classification.get_catalog_config(name)
        except NotFound:
            if usage_policy_id:
                headers = (
                    {"X-Databricks-Org-Id": workspace_id}
                    if workspace_id
                    else {}
                )
                client.api_client.do(
                    "POST",
                    f"/api/data-classification/v1/catalogs/{catalog}/config",
                    body={
                        "included_schemas": {
                            "names": desired_schemas[catalog],
                        },
                        "auto_tag_configs": [],
                        "usage_policy_id": usage_policy_id,
                    },
                    headers=headers,
                )
                existing[catalog] = desired_schemas[catalog]
            continue
        if remote.included_schemas is None:
            all_schemas.add(catalog)
            print(
                f"WARNING: {catalog} classification includes ALL schemas; preserving unset scope",
                file=sys.stderr,
            )
            continue
        existing[catalog] = sorted(set(remote.included_schemas.names or []))

    output = env_dir / "data_access" / "classification.auto.tfvars"
    lines = ["classification_existing_schemas = {"]
    for catalog, schemas in existing.items():
        rendered = ", ".join(json.dumps(schema) for schema in schemas)
        lines.append(f"  {json.dumps(catalog)} = [{rendered}]")
    lines.append("}")
    rendered_all = ", ".join(json.dumps(catalog) for catalog in sorted(all_schemas))
    lines.append(f"classification_all_schemas = [{rendered_all}]")
    output.write_text("\n".join(lines) + "\n")

    for catalog in sorted(set(existing) | all_schemas):
        print(catalog)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
