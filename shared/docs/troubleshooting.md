# Troubleshooting

This document covers dev-to-prod issues, import flows, brownfield adoption, and common provider issues.

## Dev-to-Prod Walkthrough issues

### `make generate` aborts: "could not read native classification" / empty results

With `enable_classification=true`, generation is **fail-closed**: if native `class.*` results are unreadable or empty, it aborts rather than guessing. This is intentional.
- **Have you enabled auto-tagging?** In Catalog Explorer, review detections and exclude false positives, then enable auto-tagging. If you prefer the scripted override, set `enable_auto_tagging = true` and run `make enable-classification`; review still happens in the UI.
- Confirm classification is enabled and the **scan has completed** (async, minutes to ~24h) — check `system.information_schema.column_tags` for `class.*` on your footprint.
- If your data genuinely has no format-matchable PII yet, seed realistic values or wait for the scan.
- Only to deliberately bypass (not recommended in prod): `GENERATE_ARGS='--allow-llm-sensitivity'`.

### Auto-tagging safety refusal

GenieRails refuses before applying when live auto-tagging is on in the UI but
`envs/<env>/env.auto.tfvars` sets `enable_auto_tagging = false`. Delete that line
to preserve the UI setting. Use `ALLOW_DISABLE_AUTO_TAGGING=1` only when you
really intend to turn auto-tagging off. `make promote-to` also refuses to
preserve an explicit `false` into the destination unless the same override is
provided. Values must be the unquoted HCL booleans `true` or `false`; quoted
`"true"` and `"false"` are rejected rather than coerced. Explicit `true` replaces
the UI's per-tag choices with the module's supported tag list.

### `make derive-assignments` (prod) fails on empty/unmapped classifications

By design, prod refresh rejects empty native results and unmapped `class.*` findings (a finding with no `gr_treatment` mapping). Add the missing mapping to `treatment_config.json` and re-run; it never falls back to the LLM.

### Users can't query after apply / agent not runnable

Check the **coverage check**: business `SELECT` and Genie `CAN_RUN` are only granted when a recent coverage check has passed against live tags. Run `make coverage-gate` to see what is uncovered (or a sensitive-looking untagged column blocking first exposure), fix it, then re-run `make rehearse` (dev) or `make release ENV=prod`. `business_access_enabled` is deprecated and ignored; setting it does nothing.

### First `make rehearse` fails at `verify-switched` with `INSUFFICIENT_PERMISSIONS ... USE CATALOG`

Older GenieRails versions held the tier groups' `USE_CATALOG`, `USE_SCHEMA` and `SELECT` back until the grant phase but gave the verify-access test principals no temporary access, so every tier's query failed on a first deterministic deploy. The restore then kept the new masks and tags (fail-closed, nothing granted), and `make policy-status` called that "the previous protection (none), restored". Update GenieRails and re-run the same `make rehearse ENV=<env>`. It resumes the journalled run at `verify-created`, with the test principals now getting temporary access to the checked tables (always revoked), and finishes with `grant`. Don't use `make policy-abandon` here: it refuses, because it would drop masks that are live on every column. See "verify-access before the grant phase" in [deterministic-policies.md](deterministic-policies.md).

## Importing Existing Resources (Brownfield)

If groups, tag policies, tag assignments, or FGAC policies already exist in Databricks, import them so Terraform can manage them without `already exists` errors:

```bash
make promote ENV=dev         # first: writes envs/account/abac.auto.tfvars, which the account import reads
make import ENV=account      # import account groups + tag policies into module.account
make import                  # import env-scoped governance + workspace-local resources for ENV=dev
make import ENV=prod         # import env-scoped governance + workspace-local resources for ENV=prod

cd envs/account && ../../../shared/scripts/import_existing.sh --groups-only --dry-run
cd envs/account && ../../../shared/scripts/import_existing.sh --tags-only --dry-run   # tag policies live in account
cd envs/dev/data_access && ../../../../shared/scripts/import_existing.sh --fgac-only
cd envs/dev/data_access && ../../../../shared/scripts/import_existing.sh --tag-assignments-only
```

### Brownfield workflow

For environments with existing ABAC infrastructure:

```bash
make generate
vi envs/dev/generated/masking_functions.sql
vi envs/dev/generated/abac.auto.tfvars
make promote
make import ENV=account
make import
make migrate-state           # only needed if this env already has old mixed state
make plan
make apply
```

## Common Issues

### `make` refuses `-i` or `-k`

GenieRails refuses GNU make's ignore-errors and keep-going modes
(`-i`/`--ignore-errors` and `-k`/`--keep-going`) at parse time for every
state-changing goal. Those modes can let recipe lines or sibling prerequisites
continue after a guard fails. Read-only goals still allow them: `help`, `check`,
`status`, `next`, `validate`, and `evidence OFFLINE=1`.

The guard parses `MAKEFLAGS`/`GNUMAKEFLAGS` before recipes run and ignores
command-line variable assignments after GNU make's `--` separator. It is an
accident guard, not a security boundary: it does not detect a deliberate
`--eval=.IGNORE:` override, and a caller who forges both `MAKELEVEL` and the
internal `GENIERAILS_READONLY_ERROR_FLAGS` from the environment or command line
can bypass the inherited read-only marker.

### `make import ENV=account` fails with "abac.auto.tfvars not found"

The account import reads `envs/account/abac.auto.tfvars`, and on a fresh repo nothing before it
writes that file: `make setup` and a plain `make generate` don't. Split the generated config into the layer
files, then import:

```bash
make promote ENV=dev
make import ENV=account
```

See [import existing](../IMPORT_EXISTING.md).

### Tag sync warns "live value(s) not desired by this checkout are left in place"

On a shared metastore the account tag policy (for example `gr_treatment`) holds
values other GenieRails repos, teams or legacy envs use. Rehearse, release,
apply and `make sync-tags` only add values: they leave those in place, print
them and continue. Nothing is wrong; don't remove them to silence the warning.
(Before this was fixed, the add-only sync refused here and failed
`make rehearse` even when it had nothing to add.)

To remove values that really are retired, name them:

```bash
make account-plan ALLOW_TAG_VALUE_REMOVAL=gr_treatment:redact WAREHOUSE_ID=<id>
make apply-account   # after the manifest is approved
```

At a terminal, `make rehearse ENV=dev` with the same `ALLOW_TAG_VALUE_REMOVAL`
and `WAREHOUSE_ID` runs that `account-plan` itself, names each removal and
asks `Approve and apply this account manifest? [y/N]`.

This deletes the values from the tag policy for every workspace on the
metastore. `account-plan` refuses a value that is:

| Message | Meaning / fix |
|---|---|
| `still in use: column <c.s.t.col>` (or `table`, `schema`, `catalog`, `volume`) | Something carries the tag value. Retag or untag it first (another team's? leave the value). |
| `still in use: ABAC policy <name> on <securable>` | A policy condition names the value. Change or drop that policy first. |
| `usage unknown: ...` / `ALLOW_TAG_VALUE_REMOVAL needs WAREHOUSE_ID` | The check couldn't run or couldn't see everything (no warehouse, no access to `system.information_schema`, a truncated result, a catalog, schema, table or policy listing that failed, for example on a catalog bound to another workspace). Pass a warehouse the deployer can use and grant the read (or run from a workspace that can read the named catalog); the removal is never made blind. |
| `still desired by this checkout's abac.auto.tfvars` | Remove it from `envs/account/abac.auto.tfvars` first. |
| `a deterministic generation` | Only the staged retirement removes `<treatment>__<type>__<gen>` values. |
| `not a live value of the tag policy` | Already gone, or a typo. |

`make apply-account` refuses with `the account tag-policy diff changed since
account-plan` when live values moved after planning (a planned value is no
longer live, or a value it planned to add already is), and with `removals are
chosen at account-plan, not at apply-account` when `ALLOW_TAG_VALUE_REMOVAL` is
set at apply time to a different list. Run `make account-plan` again.
`make sync-tags TAG_SYNC_MODE=replace` refuses in every checkout, legacy-only
included: the sync is add-only. `the account plan deletes tag policy [...]`
(from `account-plan` or a legacy `make apply ENV=account`) means the plan would
drop a whole key with every value; keep the key in `tag_policies`.


### "Provider produced inconsistent result after apply" (tag policies)

A known Databricks provider bug can reorder tag policy values after creation, causing a Terraform state mismatch. The tag policies themselves are usually created correctly; the failure is in provider/state reconciliation.

`make apply` reduces this significantly by:

- running `make sync-tags` through the Databricks SDK against the shared account layer before applying it
- keeping `ignore_changes = [values]` on `databricks_tag_policy`

That said, you may still occasionally see this error during the account apply, especially when creating or adopting policies for the first time. In that case:

1. Re-run `make apply`
2. If it still fails, import the affected policies into the account state and retry

Manual recovery:

```bash
cd envs/account

python3 -c "import hcl2; d=hcl2.load(open('abac.auto.tfvars')); [print(tp['key']) for tp in d.get('tag_policies',[])]" | \
  while read key; do
    ../../scripts/terraform_layer.sh account account state-rm "module.account.databricks_tag_policy.policies[\"$key\"]" 2>/dev/null || true
    ../../scripts/terraform_layer.sh account account import "module.account.databricks_tag_policy.policies[\"$key\"]" "$key" || true
  done

make apply
```

### "already exists"

Resources such as groups or tag policies already exist in Databricks. Import them so Terraform can manage them:

```bash
make import ENV=dev
```

### Destroy fails while dropping masking functions

If a previous partial destroy removed the Terraform-managed SP grant before masking functions were dropped, rerun with the current code first. The current implementation keeps the SP grant ordered correctly during destroy and can temporarily re-establish the required catalog and schema access during masking-function teardown.

If you are still recovering an older partial state:

1. Re-run `make destroy ENV=<workspace>`
2. If needed, `make apply ENV=<workspace>` first, then destroy again
3. Only destroy `ENV=account` after the workspace environments that depend on it are gone

### Generation fails with "groups is missing or empty"

In the consume-IdP-groups model, GenieRails does **not** invent groups — you supply your IdP group→tier mapping. This error almost always means the mapping is missing or a named group isn't synced, **not** LLM truncation. Retrying won't help; fix the input.

**Solutions:**
1. Pass the mapping: `make generate GENERATE_ARGS='--groups "<idp-tier-1>,<idp-tier-2>"'`. Generation refuses to proceed without it rather than inventing names.
2. Confirm each named group is **synced into the account from your IdP** (AIM/SCIM). The group-existence preflight fails loudly and names a missing group.
3. Only for a demo/greenfield account with no IdP groups: use `GENERATE_ARGS='--create-groups'` (and set `manage_groups = true` in `envs/account/env.auto.tfvars`) to let GenieRails create them.
4. To inspect the prompt without calling the model: `make generate GENERATE_ARGS='--dry-run'`.

### A column is masked with the wrong function

Sensitivity comes from **native classification** (`class.*`), and GenieRails derives one `gr_treatment` per column deterministically — so a wrong mask usually traces to the classification or the tag→treatment mapping, not an LLM guess.

**Solutions:**
1. Check the column's `class.*` tag in `system.information_schema.column_tags` — is it classified as you expect? If a type isn't recognized, add a **custom classifier**.
2. Check the `class.* → gr_treatment` mapping (`treatment_config.json`) and the strictest-wins precedence for multi-tag columns.
3. Edit `treatment_config.json` / the generated `abac.auto.tfvars`, then re-run `make coverage-gate` + `make validate-generated`.
4. The LLM only drafts rule *text* and Genie content; it does not decide which columns are sensitive.

### FGAC policy limit exceeded (100 per catalog)

Databricks enforces ~100 FGAC policies (column masks + row filters) per catalog. Option-B treatment derivation keeps you well under it by emitting **one policy per treatment per catalog** — so this is rare.

**Symptoms:** `make coverage-gate` / `make validate-generated` reports the per-catalog policy count exceeds the limit, or `make apply` fails with a provider error.

**Solutions:**
1. This shouldn't happen under Option-B; if it does, check whether policies are being emitted per-column instead of per-treatment.
2. Split tables across multiple catalogs if governance requirements differ.
3. **GenieRails never silently drops sensitive policies to fit the limit** — it fails loudly and lists the affected policies. Free capacity or re-scope; do not work around it by dropping protection.

### Terraform state conflicts or corruption

If `make apply` fails partway through, Terraform state may be inconsistent with actual cloud resources.

**Solutions:**
1. Run `make plan` to see what Terraform thinks needs to change
2. If resources exist but aren't in state: `make import ENV=<env>`
3. If state references deleted resources: run `terraform state rm <resource_address>` in the appropriate layer directory
4. As a last resort: `make destroy ENV=<env>` and re-apply from scratch
5. Never edit `.tfstate` files directly

### SQL warehouse not found or fails to create

The governance warehouse may not exist, be stopped, or fail to create.

**Solutions:**
1. If using an existing warehouse: verify `sql_warehouse_id` in `env.auto.tfvars` is correct
2. If auto-creating: ensure the SP has `CAN_MANAGE` entitlement on SQL warehouses
3. Check warehouse status in the Databricks UI — it may be stopped or in error state
4. For serverless warehouses: ensure serverless compute is enabled for your workspace

### Unity Catalog permission errors

Permission errors when fetching table DDL or applying governance.

**Solutions:**
1. Verify the SP has `MANAGE` permission on the catalog (required for tag assignments)
2. For account-level operations (groups, tag policies): the SP needs Account Admin role
3. Check `auth.auto.tfvars` credentials match the correct workspace
4. Run `make setup ENV=<env>` to verify the SP can connect

### Genie agent API errors (rate limiting, timeouts)

The Genie agent REST API may return 429 (rate limit) or timeout errors during import or config push.

**Solutions:**
1. Re-run `make generate` — transient API errors resolve on retry
2. If consistent 403 errors: the SP may not have permission to manage Genie agents
3. For large spaces with many tables: the API may timeout — reduce the number of tables per space
4. Check workspace network connectivity if behind a firewall/VPN

### Masking functions not found after deployment

FGAC policies reference masking functions that don't exist in the catalog.

**Solutions:**
1. **Run `make coverage-gate` first** — it fails precisely when a treatment's masking function is absent, and names it, before you ever apply. This is the intended guard.
2. Run `make apply` again — the masking function deployment may have failed silently on the first attempt.
3. Verify the SQL file: `cat envs/<env>/generated/masking_functions.sql` — check for syntax errors.
4. Check the catalog and schema exist: functions are created in the same catalog/schema as your tables.
5. Verify the SP has `CREATE FUNCTION` privilege on the schema.

### Column tags not appearing after apply

Tag assignments were applied but don't appear in the Databricks UI.

**Solutions:**
1. Wait 30-60 seconds — tag propagation is eventually consistent
2. Run `make sync-tags` to force synchronization via the SDK
3. Check `make plan` to see if Terraform thinks the tags need to be created
4. Verify the tag policy exists in the account layer: `make plan ENV=account`
