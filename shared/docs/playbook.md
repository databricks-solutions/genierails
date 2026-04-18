# Playbook

GenieRails puts Genie onboarding on rails — import your existing Genie Space, generate ABAC governance, and promote to production.

## Pick your starting point

| Starting point | You have... | Guide |
|---|---|---|
| **Using Claude Code** | Claude Code installed — want guided setup and generation | [Claude Code Skill](claude-code-skill.md) |
| **I already have a Genie Space** | A space configured in the Databricks UI that needs governance and promotion to prod | [From UI to Production](from-ui-to-production.md) |
| **I'm starting from scratch** | Tables in Unity Catalog, no Genie Space yet | [Quickstart](quickstart.md) |

---

## After your first deployment

### Add another Genie Space

Add a second space without re-generating existing ones:

```bash
# 1. Add Space B to env.auto.tfvars alongside your existing space(s)
vi envs/dev/env.auto.tfvars

# 2. Generate ONLY Space B's config — existing spaces are preserved
make generate SPACE="Clinical Analytics"

# 3. Review and apply
vi envs/dev/generated/abac.auto.tfvars   # verify existing spaces are unchanged
make validate-generated
make apply
```

| Situation | Command |
| --------- | ------- |
| Adding a new space without touching existing ones | `make generate SPACE="Space B"` |
| Re-tuning a single space from scratch | `make generate SPACE="Finance Analytics"` |
| Adding new groups or changing shared tag policies | `make generate` (full — reviews all spaces) |

> Per-space generation does **not** modify groups or tag_policies. If you genuinely need new groups, run full `make generate` (no `SPACE=`).

### Promote dev → prod

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

vi envs/prod/auth.auto.tfvars   # enter prod workspace credentials

make apply ENV=prod
```

For multiple catalogs:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin,dev_clinical=prod_clinical"
```

**How `DEST_CATALOG_MAP` works:**

- Comma-separated `src_catalog=dest_catalog` pairs
- The promote command auto-detects all source catalog names from `genie_spaces[*].uc_tables`
- Every detected catalog must have a mapping — the command fails clearly if any are missing

> If you followed [From UI to Production](from-ui-to-production.md), promotion was already covered in Step 4.

### Country and industry overlays

Add region-specific or industry-specific governance context:

```bash
# Country overlays (ANZ, India, Southeast Asia)
make generate COUNTRY=ANZ
make generate COUNTRY=ANZ,IN,SEA

# Industry overlays (Financial Services, Healthcare, Retail)
make generate INDUSTRY=healthcare
make generate INDUSTRY=financial_services,retail

# Both combined
make generate COUNTRY=ANZ INDUSTRY=healthcare
```

These work with any scenario — just add the flag. See [Country Overlays](country-overlays.md) and [Industry Overlays](industry-overlays.md) for details.

### Schema drift detection

After ABAC governance is deployed, table schemas may evolve. Two commands handle drift without a full `make generate` re-run:

```bash
# Detect untagged columns and stale assignments
make audit-schema ENV=dev

# Auto-classify new columns and remove stale ones
make generate-delta ENV=dev
make apply ENV=dev
```

| Schema change | What happens |
| --- | --- |
| `ALTER TABLE ADD COLUMN patient_ssn STRING` | `generate-delta` classifies and tags it |
| `ALTER TABLE DROP COLUMN old_ssn` | `generate-delta` removes the stale assignment |
| `ALTER TABLE RENAME COLUMN ssn TO tax_id` | Old assignment removed, new column classified |

---

## Advanced scenarios

These cover less common deployment patterns. Most users won't need them on day one.

### ABAC governance only (no Genie Space)

Set up groups, tag policies, column masking, row filters, and catalog grants — without creating any Genie Space. Add Genie later without changing the governance setup.

```bash
make setup
vi envs/dev/env.auto.tfvars   # list tables in uc_tables, no genie_spaces block

make generate
make validate-generated
make apply
```

### Independent BU environment

A second business unit needs its own groups, governance, and Genie spaces — not a promotion of `dev`.

```bash
make setup ENV=bu2
vi envs/bu2/env.auto.tfvars    # define the BU's genie_spaces

make generate ENV=bu2
make validate-generated ENV=bu2
make apply ENV=bu2
```

### Central governance, self-service Genie

A central Data Governance team owns ABAC policies, while BU teams self-serve their own Genie spaces. See [self-service-genie.md](self-service-genie.md) for the full guide.

```bash
# Governance team
make generate ENV=governance MODE=governance
make apply-governance ENV=governance

# BU team
make generate ENV=bu1 MODE=genie
make apply-genie ENV=bu1
```

### Import Genie Space to prod without ABAC

Import a UI-created Genie Space and deploy to production when ABAC is managed separately. See [self-service-genie.md](self-service-genie.md) for context.

```bash
make generate ENV=bu_import MODE=genie   # genie_only=true in env.auto.tfvars
make apply-genie ENV=bu_import
make promote SOURCE_ENV=bu_import DEST_ENV=bu_import_prod DEST_CATALOG_MAP="dev=prod"
make apply-genie ENV=bu_import_prod
```

---

## Destroy and reset

```bash
make destroy ENV=dev               # workspace + data_access layers
make destroy ENV=prod
make destroy ENV=account           # shared account layer (groups, tag policies)
make destroy-genie ENV=bu1         # workspace layer only
make destroy-governance ENV=gov    # data_access layer only
make clean ENV=dev                 # remove local state, keep config
make clean-all                     # remove all envs/
```

---

## How it works

See [Architecture](architecture.md) for the full reference. Quick summary:

`make apply ENV=<name>` applies in this order:

1. `envs/account/` — shared account layer (groups, tag policies)
2. `envs/<name>/data_access/` — env-scoped governance (tags, masking, grants)
3. `envs/<name>/` — workspace layer (Genie Spaces, ACLs)

The core loop:

```
inputs → make generate → review generated/ → make validate-generated → make apply
```

| File | What it contains |
| ---- | ---------------- |
| `generated/abac.auto.tfvars` | Groups, tag policies, tag assignments, FGAC policies, genie_space_configs (including per-space `acl_groups`) |
| `generated/masking_functions.sql` | SQL masking and row-filter functions |
| `generated/spaces/<key>/` | Per-space drafts (used by `make generate SPACE="..."`) |
