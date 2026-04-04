# From UI to Production

> **This is the recommended starting point.** Most users already have a Genie Space configured in the Databricks UI and want to add governance and deploy it to production.
>
> **See it in action:** The [Australian Bank Demo](../examples/aus_bank_demo/) walks through this entire flow with a realistic banking scenario — ANZ-specific masking, PCI compliance, and dev-to-prod promotion.

## What you'll achieve

1. Import your existing Genie Space configuration into code (instructions, benchmarks, SQL measures — all captured verbatim)
2. Generate ABAC governance: groups, tag policies, column masking, row filters, catalog grants
3. Review and tune the generated governance
4. Apply everything to your dev workspace
5. Promote the whole setup — governance and Genie config — to production

## What gets imported

`make generate` queries the Genie Space API and captures the full space configuration verbatim — no LLM re-writing:

| Field | Captured from API |
| ----- | ---------------- |
| Instructions (text) | ✓ |
| Sample questions | ✓ |
| Benchmarks (question + SQL) | ✓ |
| SQL filters, measures, expressions | ✓ |
| Join specs | ✓ |
| Table list | ✓ — auto-discovered from the space |
| Space title and description | ✓ |
| SQL warehouse | — kept as-is in the existing space |

The ABAC governance (groups, tag policies, tag assignments, masking functions) is generated fresh by the LLM from the discovered table DDLs.

## Step 1 — Point at your existing space

Find the Genie Space ID in the URL when viewing the space in the Databricks UI (e.g. `...genie/rooms/01ef7b3c2a4d5e6f`).

> **Prerequisite:** Complete Steps 1-2 in your cloud README ([AWS](../../aws/README.md) or [Azure](../../azure/README.md)) to set up credentials before continuing.

```bash
vi envs/dev/env.auto.tfvars
```

```hcl
# envs/dev/env.auto.tfvars
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"   # the only required field; find it in the Genie Space URL
    # name omitted     → defaults to the space title returned by the API
    # uc_tables omitted → discovered automatically from the Genie API
  },
]
```

## Step 2 — Import config and generate governance

```bash
make generate
```

This does in one step:
1. Queries the Genie Space API — discovers tables and imports existing config verbatim
2. Fetches DDLs from Unity Catalog for those tables
3. LLM generates ABAC governance (groups, tag policies, tag assignments, masking functions)
4. Writes everything to `envs/dev/generated/abac.auto.tfvars` — the imported Genie config replaces any LLM-generated Genie content

You will see output like:

```
  Querying existing Genie Space 'Finance Analytics' for config...
    Discovered 3 table(s): dev_fin.finance.customers, dev_fin.finance.transactions, ...
  Injected genie_space_configs from Genie API for: Finance Analytics

  Next steps:
    1. Review generated/TUNING.md
    2. Review and tune generated/abac.auto.tfvars
    3. make apply
```

> **Required manual step:** Copy the discovered tables into `envs/dev/data_access/env.auto.tfvars` so that UC grants and masking functions are applied to them:
>
> ```hcl
> # envs/dev/data_access/env.auto.tfvars
> uc_tables = [
>   "dev_fin.finance.customers",
>   "dev_fin.finance.transactions",
>   # ... (as printed by make generate)
> ]
> ```

## Step 3 — Review, tune, and apply

```bash
vi envs/dev/generated/abac.auto.tfvars
# - Review the imported genie_space_configs (instructions, benchmarks, etc.)
# - Review and tune the generated groups, tag_assignments, fgac_policies
# - Check acl_groups per space — controls which groups can run each Genie Space
#   (see "Per-space Genie ACLs" below)

vi envs/dev/generated/masking_functions.sql
# Review and iterate on generated masking and row-filter functions.

make validate-generated
make apply
```

> `make apply` attaches to the existing space (does not create or delete it), applies ABAC governance, per-space ACLs, and pushes any changes to the space config (instructions, benchmarks, etc.) back to the API.

### Per-space Genie ACLs

Each space in `genie_space_configs` has an `acl_groups` field that controls which groups get `CAN_RUN` access. The LLM assigns groups based on which FGAC policies reference each space's tables:

```hcl
genie_space_configs = {
  "Finance Analytics" = {
    acl_groups = ["Finance_Analyst", "Manager"]   # only these groups can run this space
    # ... instructions, benchmarks, etc.
  }
  "Clinical Analytics" = {
    acl_groups = ["Clinical_Staff", "Manager"]    # different groups for this space
    # ...
  }
}
```

**Review checklist:**
- Verify each space's `acl_groups` includes all groups that need access
- Groups not listed are excluded from that space (they cannot run queries)
- If `acl_groups` is empty or omitted, all groups get access (backward compatible)
- `acl_groups` entries must match group names defined in the `groups` block

## Step 4 — Promote to prod

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin"

vi envs/prod/auth.auto.tfvars   # prod workspace credentials
vi envs/prod/env.auto.tfvars
```

For prod, leave `genie_space_id` empty to create a brand-new prod space from the promoted config, or set it to an existing prod space ID to attach:

```hcl
# envs/prod/env.auto.tfvars  (written by make promote, edit as needed)
genie_spaces = [
  {
    name           = "Finance Analytics"
    genie_space_id = ""            # empty → tool creates new prod space with the promoted config
    uc_tables = [
      "prod_fin.finance.customers",
      "prod_fin.finance.transactions",
    ]
  },
]
```

```bash
make apply ENV=prod
# Creates the prod Genie Space with the full promoted config:
# governance (groups, tags, masking) + Genie content (instructions, benchmarks, SQL)
```

---

## Multi-space import

Import multiple spaces in a single `make generate` by listing them all with `genie_space_id`:

```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"   # Finance Analytics
  },
  {
    genie_space_id = "02ab9c1d3e4f5a6b"   # Executive Dashboard
  },
]
```

Each space's config is fetched independently. All spaces get their governance generated in the same LLM call, and all are promotable together.

---

## Good to know

- **API async delay**: The Genie API's `serialized_space` field may take 1–3 minutes to populate for newly created spaces. The tool retries automatically for up to ~4 minutes. If the space was just created moments ago, wait a minute before running `make generate`.
- **Destroy safety**: `make destroy` never deletes an attached space (`genie_space_id` set). Only spaces created by this tool (empty `genie_space_id`) are destroyed.
- **Config drift**: After the first import, `abac.auto.tfvars` is the source of truth. Changes made in the UI will not automatically sync back — re-run `make generate` (with `genie_space_id`) to re-import if needed.

---

## What's next?

- [Add another Genie Space](playbook.md#add-another-genie-space) — incremental generation without touching existing spaces
- [Country & industry overlays](playbook.md#country-and-industry-overlays) — region-specific or industry-specific governance
- [Schema drift detection](playbook.md#schema-drift-detection) — handle table changes after initial deployment
- [Advanced scenarios](playbook.md#advanced-scenarios) — ABAC-only, self-service Genie, independent BU environments
- [Version control your configs](version-control.md) — what to commit, version pinning, running Terraform directly
