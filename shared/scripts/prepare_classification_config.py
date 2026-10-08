#!/usr/bin/env python3
"""Preserve and adopt an existing catalog classification config, if present."""

import json
import sys
from pathlib import Path

import hcl2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

SHARED_ROOT = Path(__file__).resolve().parent.parent
if str(SHARED_ROOT) not in sys.path:
    sys.path.insert(0, str(SHARED_ROOT))

from scripts.footprint import FootprintError, resolve_footprint


def _load(path: Path) -> dict:
    with path.open() as handle:
        return hcl2.load(handle)


def _value(config: dict, key: str) -> str:
    value = config.get(key, "")
    if isinstance(value, list):
        value = value[0] if value else ""
    return str(value).strip()


def _auto_tag_configs(remote) -> list[dict[str, str]]:
    """Return provider-shaped auto-tag settings without changing UI ownership."""
    configs = []
    for item in getattr(remote, "auto_tag_configs", None) or []:
        raw = item.as_dict() if hasattr(item, "as_dict") else item
        tag = raw.get("classification_tag")
        mode = raw.get("auto_tagging_mode")
        if tag and mode:
            configs.append({
                "classification_tag": str(tag),
                "auto_tagging_mode": getattr(mode, "value", str(mode)),
            })
    return sorted(configs, key=lambda item: item["classification_tag"])


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

    try:
        tables = resolve_footprint(env_dir)
    except FootprintError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
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
    existing_auto_tags: dict[str, list[dict[str, str]]] = {}
    all_schemas: set[str] = set()
    usage_policy_id = _value(auth, "serverless_usage_policy_id")
    for catalog in catalogs:
        name = f"catalogs/{catalog}/config"
        try:
            remote = client.data_classification.get_catalog_config(name)
        except NotFound:
            if usage_policy_id:
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
                )
                existing[catalog] = desired_schemas[catalog]
                existing_auto_tags[catalog] = []
            continue
        existing_auto_tags[catalog] = _auto_tag_configs(remote)
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
    lines.append("classification_existing_auto_tag_configs = {")
    for catalog, configs in existing_auto_tags.items():
        lines.append(f"  {json.dumps(catalog)} = [")
        for config in configs:
            lines.append("    {")
            lines.append(f"      classification_tag = {json.dumps(config['classification_tag'])}")
            lines.append(f"      auto_tagging_mode   = {json.dumps(config['auto_tagging_mode'])}")
            lines.append("    },")
        lines.append("  ]")
    lines.append("}")
    output.write_text("\n".join(lines) + "\n")

    for catalog in sorted(set(existing) | all_schemas):
        print(catalog)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
