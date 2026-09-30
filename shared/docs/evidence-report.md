# Compliance evidence report

`make evidence ENV=<env>` writes a versioned JSON artifact and matching Markdown summary under `envs/<env>/generated/evidence/`. Offline mode is the default: it records the configured column footprint and tags without creating a Databricks client.

To capture deployed Unity Catalog tags, masks, grants, and column observation status, opt into live collection explicitly:

```bash
GENIERAILS_EVIDENCE_INTEGRATION=1 make evidence ENV=prod WAREHOUSE_ID=<sql-warehouse-id>
```

Set `GENIERAILS_EVIDENCE_APPROVED_BY` and `GENIERAILS_EVIDENCE_APPROVED_AT` to populate the approval header. Each column entry contains its classification scan status/time, detected tags, applied mask and row-filter policy names, and effective column, table, schema, and catalog grants. The `schema_version` field identifies the artifact contract; JSON is canonical and Markdown is its review-friendly rendering.

Live collection uses the Databricks SDK statement execution API and Unity Catalog `system.information_schema`. It is intentionally unavailable unless `GENIERAILS_EVIDENCE_INTEGRATION` is truthy, keeping unit tests and normal offline generation credential-free.
