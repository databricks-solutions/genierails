# Advanced Usage

This document covers optional and advanced workflows that most first-time users can skip.

## Generation Options

Generation consumes your IdP-synced groups, so it needs a group→tier mapping (`--groups`) unless you explicitly opt into demo group creation (`--create-groups`). Sensitivity comes from native `class.*` classification, not from these flags.

```bash
make generate GENERATE_ARGS='--tables a.b.* c.d.e --groups "Finance_Analyst,Clinical_Staff"'
make generate GENERATE_ARGS='--dry-run'
```

If you want to run the script directly, do it from inside the env workspace and call the root-owned script:

```bash
cd envs/dev
python ../../generate_abac.py --tables "a.b.*" "c.d.e" --groups "Finance_Analyst,Clinical_Staff"
python ../../generate_abac.py --dry-run
```

Pass the group→tier mapping through Make:

```bash
make generate GENERATE_ARGS='--groups "Finance_Analyst,Clinical_Staff"'
```

## IdP-Synced Groups (default)

**GenieRails consumes IdP-owned groups by default; it never mints them in the normal path.** Ownership is split cleanly:

- **The IdP owns groups and membership.** Enable [AIM](#prerequisite-sync-your-idp-groups) (or SCIM where AIM isn't available) so your identity provider — Okta, Azure AD/Entra ID, etc. — syncs the access-tier groups into the Databricks account.
- **GenieRails owns grants and ABAC.** It looks the synced groups up by name and attaches tags, FGAC policies, and Genie agent ACLs to them.

### Prerequisite: sync your IdP groups

Before running `make apply`, the access-tier groups must already exist as account-level groups, synced from your IdP:

- **AIM (Automatic Identity Management)** — the preferred path. Databricks provisions users and groups from your IdP automatically.
- **SCIM provisioning** — use this where AIM isn't available for your IdP. Configure a SCIM connector from the IdP to the Databricks account.

### How consume-by-default works

- The **group→tier mapping** (which existing IdP group fills each access tier) is the primary, expected input. Provide it with `--groups`:

  ```bash
  make generate GENERATE_ARGS='--groups "acme-finance-readers,acme-clinical-staff,acme-compliance"'
  ```

  The LLM uses these exact names in generated FGAC policies and Genie agent ACLs — it does not invent new ones. (Column *sensitivity* comes from native `class.*` classification, not from groups.)
- `manage_groups` defaults to **`false`** everywhere (account, `data_access`, and workspace layers). All three layers look groups up by name via `data "databricks_group"`; none create them.
- **Group-existence preflight:** `make generate` verifies every referenced group is synced into the account. If one is missing, generation **fails loudly and names the missing group**, telling you to enable AIM/SCIM (or fix the name) — rather than silently producing a grant that matches nobody. (The preflight is skipped only when account credentials aren't available; the account layer's `data "databricks_group"` lookup then fails at apply time instead.)
- `group_members` stays empty in `envs/account/abac.auto.tfvars` — the IdP owns membership.

### Opt-in group creation (demo / greenfield only)

For a demo or greenfield account with no IdP syncing groups yet, GenieRails can still mint them. This is **opt-in and off by default**:

```bash
make generate GENERATE_ARGS='--create-groups'
```

`--create-groups` lets the LLM invent access-tier names and skips the preflight. To have Terraform create them, also set `manage_groups = true` in `envs/account/env.auto.tfvars` (the only place that flag belongs). Keep workspace and `data_access` envs on the default `manage_groups = false` (lookup-only) regardless.

## ABAC-Only Mode (No Genie agent)

See [playbook.md — ABAC governance only](playbook.md#abac-governance-only-no-genie-agent) for the full step-by-step.

## Existing Masking Functions

If you have pre-existing masking SQL UDFs, the tool can incorporate them:

1. Run `make generate` so the AI creates `masking_functions.sql` and `abac.auto.tfvars` in `envs/dev/generated/`
2. Edit `envs/dev/generated/masking_functions.sql` and replace generated UDF definitions with your existing functions
3. Update `function_name`, `function_catalog`, and `function_schema` in `envs/dev/generated/abac.auto.tfvars` to match your existing UDFs
4. Run `make coverage-gate` (confirm every classified column still has a covering mask), then `make apply`

## Multi-Environment File Layout

For day-to-day workflows (promote, independent BU, self-service Genie) see [playbook.md](playbook.md). This section documents the directory structure those workflows produce.

Workspace environment names can be anything: `dev`, `staging`, `prod`, `bu2`, or something business-unit-specific. `account` and `data_access` are reserved names.

```text
envs/
  account/
    auth.auto.tfvars
    env.auto.tfvars
    abac.auto.tfvars
    terraform.tfstate

  dev/
    auth.auto.tfvars
    env.auto.tfvars
    data_access/
      auth.auto.tfvars
      env.auto.tfvars
      abac.auto.tfvars
      masking_functions.sql
      terraform.tfstate
    abac.auto.tfvars
    ddl/
    generated/
    .genie_space_id
    terraform.tfstate

  prod/
    (same structure as dev/)

roots/
  account/
  data_access/
  workspace/
```

Each env keeps its own Terraform state and local artifacts. `account` is the only shared layer; governance and workspace files are isolated per environment under `envs/<env>/data_access/` and `envs/<env>/`.

`make sync-tags` runs against the shared account layer because tag policy definitions are account-scoped.

## Migrating an Existing Root-Based Workspace

If you already used the old root-local workflow, migrate it once before using the new default env dispatch:

```bash
make migrate-root-to-env ENV=dev
make migrate-state ENV=dev
```

That moves root working files into `envs/dev/` and rewrites any legacy top-level Terraform addresses into the new layered module addresses so future `make generate` and `make apply` commands continue from the same environment layout without forced recreation.

## Examples

Pre-built examples with 3-layer configs (account, data access, workspace):
- `examples/dev_to_prod/` — **the canonical end-to-end walkthrough** (native classification → coverage check → safe dev→prod promotion) ([README](../examples/dev_to_prod/README.md))
- `examples/finance/` — finance validation fixture: PII, PCI, and AML governance
- `examples/healthcare/` — healthcare fixture: PHI, PII, and regional row filters ([walkthrough](../examples/healthcare/healthcare_walkthrough.md))
- `examples/legacy/{aus,india,asean}_bank_demo/` — **older LLM-overlay demos** (region-specific: ANZ/TFN, India/Aadhaar-PAN-GSTIN-UPI, ASEAN 6-country IDs). Superseded by the dev-to-prod walkthrough; kept for reference.
