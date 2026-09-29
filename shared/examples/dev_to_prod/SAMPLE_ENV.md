[← Back to the Dev-to-Prod Walkthrough](README.md)

# Sample Environment Setup (optional)

**No tables or Genie agent of your own?** Expand this to create a sample schema (three tables of realistic synthetic PII) + a sample agent and get the exact values to paste into `envs/dev/env.auto.tfvars`. **Skip it if you have your own.**

Do [Phase 0](README.md#phase-0--set-up-dev) first, then this, then continue to [Phase 1](README.md#phase-1--dev-scan-draft-the-rules-test-them).

```bash
cd ../shared/examples/dev_to_prod          # from the cloud root (aws/ or azure/); return with 'cd ../../../aws' afterward
python -m pip install -r requirements.txt
python setup_sample_env.py --catalog dev_finance --warehouse-id <your-warehouse-id>
```

**Auth is optional to specify.** It uses a **Databricks CLI profile** — separate from the deploying Service Principal `make` uses (that lives in `auth.auto.tfvars`). No flag → your **default** CLI profile (or `DATABRICKS_HOST`/`DATABRICKS_TOKEN`); add `--profile <name>` only for a *named* profile.

The script prints the `uc_tables`, `genie_spaces`, and `sql_warehouse_id` snippet — paste it into `envs/dev/env.auto.tfvars`. It does **not** create access-tier groups, so in Phase 1 pass `--groups` with existing names or use `--create-groups`. Re-runs are safe. To remove only what it created:

```bash
python teardown_sample_env.py --catalog dev_finance
# Equivalent: add --teardown to the setup command.
```

Use `--help` for `--host`, `--schema`, `--rows`, and env-var alternatives. Then `cd ../../../aws` (or the azure path) and continue.
