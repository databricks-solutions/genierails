[← Back to the Dev-to-Prod Walkthrough](README.md)

# Sample Environment Setup (optional)

**No tables or Genie agent of your own?** Expand this to create a sample schema (three tables of realistic synthetic PII) + a sample agent and get the exact values to paste into `envs/dev/env.auto.tfvars`. **Skip it if you have your own.**

Do [Phase 0](README.md#phase-0--dev-set-up) steps 1–2 first, then this, then Phase 0 step 3 (the import) with the printed agent ID.

```bash
cd ../shared/examples/dev_to_prod          # from the cloud root (aws/ or azure/); return with 'cd ../../../aws' afterward
python -m pip install -r requirements.txt
python setup_sample_env.py --catalog dev_finance --warehouse-id <your-warehouse-id>
```

**Auth is optional to specify.** It uses a **Databricks CLI profile** — separate from the deploying Service Principal `make` uses (that lives in `auth.auto.tfvars`). No flag → your **default** CLI profile (or `DATABRICKS_HOST`/`DATABRICKS_TOKEN`); add `--profile <name>` only for a *named* profile.

The script prints a `uc_tables`, `genie_spaces`, and `sql_warehouse_id` snippet — in `envs/dev/env.auto.tfvars`, **replace** the template's `genie_spaces` and `sql_warehouse_id` lines with it (each setting may appear only once). By default it does **not** create access-tier groups, so set `access_tier_groups` to existing names. No IdP groups (a demo account)? Add `--create-groups --account-id <account-id>`: it also creates three demo account groups (`dev_to_prod_payments_ops`, `dev_to_prod_regional_analysts`, `dev_to_prod_viewers`) and prints the matching `access_tier_groups` line (this needs Account Admin, via environment credentials or `--account-profile`). Re-runs are safe. To remove only what it created:

```bash
python teardown_sample_env.py --catalog dev_finance
# Equivalent: add --teardown to the setup command.
```

**Prod needs the same tables.** In [Phase 2](README.md#phase-2--prod-set-up-and-promote-rules), seed the prod catalog before scanning it — tables only, because `make promote` creates prod's agent:

```bash
python setup_sample_env.py --host <prod-workspace-url> --catalog prod_finance --warehouse-id <prod-warehouse-id> --skip-agent
```

Use `--help` for `--host`, `--schema`, `--rows`, and env-var alternatives. Then `cd ../../../aws` (or the azure path) and continue.
