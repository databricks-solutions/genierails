# Using GenieRails with Claude Code

GenieRails includes a built-in Claude Code skill that turns the multi-step CLI workflow into a guided conversation. Instead of memorizing make targets and editing config files manually, you can say "set up genierails" or "generate ABAC for my tables" and the assistant walks you through it.

## What You Get

**Automatic (no setup):** When you open Claude Code in the GenieRails repo, `CLAUDE.md` auto-loads — giving Claude project context, safety rules, and command knowledge. You can immediately ask questions like "how do I promote to prod?" and get accurate answers.

**Interactive skill (`/genierails`):** The full guided assistant with state detection, workflow routing, overlay auto-detection, and step-by-step walkthroughs.

## Setup

### Step 1: Install Claude Code

If you haven't already, install [Claude Code](https://docs.anthropic.com/en/docs/claude-code) (CLI, desktop app, or IDE extension).

### Step 2: Register the Skill

Add the GenieRails skill path to your Claude Code settings. Run this from the repo root:

```bash
# Add to your project-local Claude Code settings
claude config add --project skills "$(pwd)/shared/.claude/skills/genierails"
```

### Step 3: Verify

Open Claude Code in the `aws/` or `azure/` directory and type `/genierails`. You should see the skill activate and report your project status.

---

## Usage Examples

### Check Status

Ask for a status check at any time to see where you are in the workflow:

```
You: /genierails status

Claude: [aws/dev] Setup:✓ Creds:✓ Config:✓ Generated:✗ Applied:✗
        → Next step: make generate

        Environments: dev (configured), account (shared)
        Genie Spaces: 1 defined (Sales Analytics — 3 tables)
        Overlays: none configured
```

### First-Time Setup

Start from scratch — the skill detects credentials, scaffolds config, and validates connectivity:

```
You: help me set up genierails

Claude: You're in aws/. Let me bootstrap your environment.
        Running `make setup`... done.

        I found a Databricks profile in ~/.databrickscfg:
          Host: https://dbc-abc123.cloud.databricks.com
          Account ID: 0d26daa6-5e44-...

        What's the client_id for your service principal?
        (Find it in Account Console → Service principals → your SP)

You: 30010312-5ded-434a-af59-efad8ec5161d

Claude: And the client_secret? (I won't display it after you provide it)

You: dapi_xxxxx...

Claude: ✓ Credentials saved to envs/dev/auth.auto.tfvars
        ✓ Connected as: my-service-principal@databricks.com

        Which Unity Catalog tables should this Genie Space cover?
        Use fully qualified names: catalog.schema.table (or catalog.schema.*)

You: dev_fin.finance.transactions, dev_fin.finance.customers, dev_fin.finance.accounts

Claude: ✓ Wrote envs/dev/env.auto.tfvars with Genie Space "Finance Analytics" (3 tables)

        Setup complete! Next: run /genierails generate
```

### Generate ABAC Governance

The skill auto-detects which country/industry overlays to suggest based on your table columns:

```
You: /genierails generate

Claude: Pre-flight checks:
        ✓ Credentials valid
        ✓ 3 tables found in Unity Catalog

        I see columns named 'bsb', 'tfn', 'medicare_number' in your tables.
        These are Australian identifiers. Suggest adding COUNTRY=ANZ overlay?

You: yes

Claude: Running: make generate COUNTRY=ANZ

        ✓ Generation complete (47s):
          - 4 groups (Data_Analysts, Data_Engineers, Compliance_Officer, Finance_Admin)
          - 3 tag policies (pii_level, pci_level, financial_sensitivity)
          - 14 tag assignments
          - 8 FGAC policies (6 column masks, 2 row filters)
          - 12 masking functions (including mask_tfn, mask_medicare, mask_bsb)
          - 1 Genie Space config

        Review the generated files:
          envs/dev/generated/abac.auto.tfvars    — governance config
          envs/dev/generated/masking_functions.sql — SQL masking functions

        Next: /genierails apply (or review files first)
```

### Validate and Apply

```
You: /genierails apply

Claude: Running validation first: make validate-generated COUNTRY=ANZ

        ✓ 13 passed, 1 warning, 0 errors
        ⚠ Warning: SQL file defines functions not used by any policy (mask_passport)
           — this is normal, the function is available but not assigned to columns

        Ready to apply. This will deploy to your dev workspace:
          1. Account layer: 4 groups, 3 tag policies
          2. Data access layer: 14 tag assignments, 8 FGAC policies, 12 SQL functions
          3. Workspace layer: 1 Genie Space with ACLs

        Proceed? (y/n)

You: y

Claude: Applying...
        ✓ Account layer applied (32s)
        ✓ Data access layer applied (45s)
        ✓ Workspace layer applied (28s)

        Deployed! Open your Genie Space:
        https://dbc-abc123.cloud.databricks.com/genie

        Test it by asking a question. Non-privileged users should see
        masked values for TFN, Medicare, and BSB columns.
```

### Promote to Production

```
You: promote to prod

Claude: I'll set up your production deployment.

        Detected source catalogs: dev_fin
        Suggested mapping: dev_fin → prod_fin

        Is this correct? (or provide your own)

You: yes

Claude: Running: make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_fin=prod_fin"

        ✓ Promotion complete. Created envs/prod/ with remapped config.

        Next steps:
        1. Edit envs/prod/auth.auto.tfvars with your prod workspace credentials
        2. Run: make apply ENV=prod

        Want me to help configure prod credentials?
```

### Import an Existing Genie Space

```
You: I have an existing Genie Space I want to import

Claude: What's the Genie Space URL? (e.g. https://xxx.databricks.com/genie/rooms/01ef7b3c2a4d5e6f)

You: https://dbc-abc123.cloud.databricks.com/genie/rooms/01ef7b3c2a4d5e6f

Claude: Space ID: 01ef7b3c2a4d5e6f

        I'll add it to your env config and auto-discover its tables.

        ✓ Updated envs/dev/env.auto.tfvars with genie_space_id
        Running: make generate COUNTRY=ANZ

        ✓ Discovered 5 tables from the Genie API
        ✓ Imported space config verbatim (instructions, benchmarks, measures)
        ✓ Generated ABAC governance from table DDLs

        The existing space will NOT be modified — only group ACLs will be applied.
        Next: review generated files, then /genierails apply
```

### Schema Drift

```
You: my tables changed, how do I update?

Claude: Let me check for schema drift.
        Running: make audit-schema ENV=dev

        Drift detected:
        + dev_fin.finance.customers.loyalty_tier (new column — STRING)
        + dev_fin.finance.transactions.channel (new column — STRING)
        - dev_fin.finance.accounts.old_status (removed column)

        2 new columns need governance, 1 stale assignment to clean up.
        Recommend: make generate-delta (incremental — keeps existing tuning)

        Run it? (y/n)
```

### Run a Demo

```
You: I want to try the Australian bank demo

Claude: I'll set up the aus_bank_demo. This creates:
        - A fresh Databricks workspace with test banking data
        - Customers, accounts, transactions, credit cards tables
        - ANZ-specific governance (TFN, Medicare, BSB masking)

        Prerequisites:
        ✓ shared/scripts/account-admin.aws.env exists
        ✗ AWS CLI auth — please run: aws sso login --profile <your-profile>

        Once AWS is authenticated, I'll run the provisioning (~15 min).
```

### Troubleshooting

Paste an error and the skill identifies the fix:

```
You: I'm getting "already exists" errors when running make apply

Claude: This means Databricks resources (groups or tag policies) already exist
        outside of Terraform's state. Import them:

        make import ENV=account    # Import account-level resources
        make import ENV=dev        # Import env-scoped resources

        Then retry: make apply
```

---

## Quick Reference

| What you want | What to say |
|---|---|
| Check progress | `/genierails status` |
| Initial setup | `help me set up genierails` |
| Generate governance | `/genierails generate` |
| Validate and deploy | `/genierails apply` |
| Promote to prod | `promote to prod` |
| Import existing space | `import my existing Genie Space` |
| Check for schema drift | `my tables changed` |
| Run a demo | `try the aus bank demo` |
| Add another space | `add a new Genie Space` |
| Add country overlay | `add ANZ overlay` |
| Fix an error | paste the error message |
| Understand architecture | `how does the 3-layer model work?` |

## Tips

- **You don't need `/genierails` for everything.** The `CLAUDE.md` context means Claude already understands the project. Just ask naturally: "how do I add a second space?" or "what does make promote do?"
- **The skill auto-detects state.** You don't need to tell it where you are in the workflow — it checks your `envs/` directory and knows what's done and what's next.
- **Paste errors directly.** Don't describe the error — paste the full output. The skill matches error patterns and suggests specific fixes.
- **Country overlays are suggested automatically** during generation based on your table column names. You don't need to know which overlay to use.
