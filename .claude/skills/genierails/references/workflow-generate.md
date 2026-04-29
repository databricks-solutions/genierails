# Smart Generation Workflow

## Pre-Flight Checks

Before running generation, verify:

1. **Credentials work**: read `envs/dev/auth.auto.tfvars`, check fields are non-empty
2. **Tables exist**: read `envs/dev/env.auto.tfvars`, verify `genie_spaces` has entries with `uc_tables`
3. **Cloud directory**: confirm cwd is `aws/` or `azure/`

## Auto-Detect Overlays

Read the table column names to suggest country/industry overlays. Check the DDL cache at `envs/dev/ddl/` if it exists, or parse table names from `env.auto.tfvars`.

### Country Detection

Look for country-specific column name patterns in the tables:

| Column patterns | Suggested overlay | Region |
|---|---|---|
| tfn, tax_file_number, medicare, bsb, abn, ird, nhi | `COUNTRY=ANZ` | Australia & NZ |
| aadhaar, aadhar, pan_number, gstin, voter_id, uan, ifsc | `COUNTRY=IN` | India |
| nric, fin_number, mykad, nik, thai_id, philsys, cccd | `COUNTRY=SEA` | SE Asia |

### Industry Detection

| Column patterns | Suggested overlay | Industry |
|---|---|---|
| credit_card, cvv, card_number, aml_risk, account_number, bsb, routing | `INDUSTRY=financial_services` | Banking/Finance |
| diagnosis, mrn, icd_code, procedure, hipaa, phi, encounter | `INDUSTRY=healthcare` | Healthcare |
| sku, product_id, cart, order_id, loyalty_points | `INDUSTRY=retail` | Retail |

If patterns are detected, suggest the overlay and ask user to confirm before proceeding.

## Build the Command

Construct the `make generate` command based on detected parameters:

```bash
make generate [SPACE="Name"] [COUNTRY=ANZ] [INDUSTRY=financial_services] [MODE=governance|genie]
```

Show the user the constructed command and ask for confirmation before running.

### Parameter Guide

- **No parameters**: full generation for all spaces (most common first-time use)
- **SPACE="Name"**: regenerate only one space — use when adding a second space or re-tuning one
- **COUNTRY**: add region-specific masking (can be comma-separated: `COUNTRY=ANZ,SEA`)
- **INDUSTRY**: add industry-specific compliance patterns
- **MODE=governance**: generate only account+data_access config (for central governance teams)
- **MODE=genie**: generate only workspace config (for BU teams in self-service mode)

## Execute and Summarize

Run the command. Generation typically takes 1-3 minutes (LLM call + autofix passes).

After completion, parse the output and provide a summary:
- Number of groups created
- Number of tag policies
- Number of tag assignments
- Number of FGAC policies (column masks + row filters)
- Number of masking functions in SQL
- Number of Genie Space configs

## Post-Generation Review

Guide the user to review the generated files:

### `envs/<env>/generated/abac.auto.tfvars`

Key sections to review:
- **groups**: team-based access groups (analysts, engineers, compliance, etc.)
- **tag_policies**: tag key definitions with allowed values (pii_level, pci_level, etc.)
- **tag_assignments**: which columns get which tags (the core governance mapping)
- **fgac_policies**: column masking and row filter rules tied to tag values
- **genie_space_configs**: per-space instructions, benchmarks, measures, ACL groups

### `envs/<env>/generated/masking_functions.sql`

Review the SQL masking functions. Common things to tune:
- Masking granularity (e.g., show last 4 vs last 3 digits)
- Row filter logic for compliance-restricted rows
- Whether country-specific functions are present if overlays were used

### `envs/<env>/generated/TUNING.md`

If this file exists, read it and summarize the key review items for the user.

## Next Steps

"Generation complete! Review the files above, then:
1. `make validate-generated` — check for errors
2. `make apply` — deploy to your workspace
Or use `/genierails apply` for guided deployment."
