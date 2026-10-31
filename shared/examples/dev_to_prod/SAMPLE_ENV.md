[← Back to the Dev-to-Prod Walkthrough](README.md)

# Sample Environment Setup (optional)

**No tables or Genie agent of your own?** Expand this to create a sample schema (three tables of realistic synthetic PII) + a sample agent and get the exact values to paste into `envs/dev/env.auto.tfvars`. **Skip it if you have your own.**

In the walkthrough's [Dev step 1](README.md#phase-0--dev-set-up), run `make setup ENV=dev`, copy the template and fill in `auth.auto.tfvars`; then run this, and set the Agent ID it prints.

```bash
cd ../shared/examples/dev_to_prod          # from the cloud root (aws/ or azure/); return with 'cd ../../../aws' afterward
python -m pip install -r requirements.txt
python setup_sample_env.py --catalog dev_finance --warehouse-id <your-warehouse-id>
```

**Auth is optional to specify.** It uses a **Databricks CLI profile** — separate from the deploying Service Principal `make` uses (that lives in `auth.auto.tfvars`). No flag → your **default** CLI profile (or `DATABRICKS_HOST`/`DATABRICKS_TOKEN`); add `--profile <name>` only for a *named* profile.

The script prints a `uc_tables` and `sql_warehouse_id` snippet — in `envs/dev/env.auto.tfvars`, **replace** the template's `sql_warehouse_id` line with it (each setting may appear only once) — and the sample agent's file, `envs/dev/agents/Dev-to-Prod Walkthrough.auto.tfvars`, with its `genie_space_id` and `acl_groups` (who may run it; required, `[]` means nobody). Write that file in place of the `envs/dev/agents/Finance Analytics.auto.tfvars` that `make setup ENV=dev` copied from `agent.auto.tfvars.example`: delete that one (an agent file that still holds placeholders stops `make generate`). By default it does **not** create access-tier groups, so set `access_tier_groups` to existing names and replace the `acl_groups` placeholder with the ones that may run the agent. No IdP groups (a demo account)? Add `--create-groups --account-id <account-id>`: it also creates three demo account groups (`dev_to_prod_payments_ops`, `dev_to_prod_regional_analysts`, `dev_to_prod_viewers`) and prints the matching `access_tier_groups` line and an agent file whose `acl_groups` lists them (this needs Account Admin, via environment credentials or `--account-profile`).

**Re-runs are safe from the same clone.** What the script created is recorded in `shared/examples/dev_to_prod/.dev_to_prod_sample_env.json`, a git-ignored file in *this clone* (its path is printed at the end of each run); re-runs and teardown use it. A fresh clone has no such file, so it refuses a schema an earlier clone created (`already exists but is not tracked as owned by this script`): copy the earlier clone's `.dev_to_prod_sample_env.json` into the new clone's `shared/examples/dev_to_prod/` (merge the entries if the new clone already has one) and re-run, or choose another `--schema`. Keep the file for as long as the sample data exists.

To remove only what it created:

```bash
python teardown_sample_env.py --catalog dev_finance
# Equivalent: add --teardown to the setup command.
```

With `--create-groups`, teardown deletes exactly the groups it created, by their recorded IDs and only in the account it recorded (it refuses a different `--account-id` / `DATABRICKS_ACCOUNT_*`). No extra flags are needed.

State written before group IDs were recorded lists groups by name only; teardown keeps those groups and lists them — delete them in the Account Console, or opt in with `--delete-legacy-groups-by-name --account-id <account-id>` (exact-name match, so it can also remove a different group that reused the name).

**Prod needs the same tables.** In the walkthrough's [Prod section](README.md#prod), seed the prod catalog before scanning it — tables only, because `make release ENV=prod` creates prod's agent later:

```bash
python setup_sample_env.py --profile <prod-workspace-profile> --catalog prod_finance --warehouse-id <prod-warehouse-id> --skip-agent
```

Use the prod workspace's CLI profile (for example `genierails-prod` from the [prerequisites](../../docs/prerequisites.md)). `--host <prod-workspace-url>` alone takes its credentials from the environment or your default profile, which belong to another workspace when you keep one profile per workspace.

Use `--help` for `--host`, `--schema`, `--rows`, and env-var alternatives. Then `cd ../../../aws` (or the azure path) and continue.
