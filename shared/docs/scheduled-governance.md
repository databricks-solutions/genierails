# Scheduled Steady-State Governance

Roadmap item: **scheduled prod re-scan + drift/coverage on a schedule.**

GenieRails governance is normally re-derived on demand — a developer runs
`make audit-schema`, `make generate-delta`, and `make validate-generated` when a
schema changes (see [playbook.md](playbook.md) and [cicd.md](cicd.md)). The
scheduled governance job runs those **same steady-state entrypoints** on a cron
so drift is detected (and incrementally re-classified) without waiting for
someone to kick off a scan.

It is an opt-in **wrapper only** — it adds no new re-derive logic. A single job
task runs the three steps **in one process** via
[`scripts/run_scheduled_governance.py`](../scripts/run_scheduled_governance.py)
`--step all`, so they share one Git checkout and the coverage step validates the
config the delta step just regenerated:

| Step       | Wraps                           | Equivalent make target      |
| ---------- | ------------------------------- | --------------------------- |
| `audit`    | `scripts/audit_schema_drift.py` | `make audit-schema`         |
| `delta`    | `generate_abac.py --delta`      | `make generate-delta`       |
| `coverage` | `validate_abac.py`              | `make validate-generated` (falls back to `make validate` for split configs) |

The steps run in order `audit → delta → coverage` within the one task. `audit`
exits non-zero when drift is found; that non-zero code is remembered, so a **red
run is the drift signal** even though `delta` still runs and re-derives the
drift and `coverage` re-validates the regenerated config. `coverage` fails
loudly (never a silent pass) if no ABAC config exists to validate.

> **Why one task, not three?** Databricks Git-backed tasks each get their own
> checkout. If the steps were separate tasks, `delta` would write to its own
> working tree and `coverage` would start from a fresh checkout that never saw
> the regenerated file. Running them in one process keeps them on one working
> tree.

## Where the env config comes from (required)

The Git checkout provides the **code** only. The repo's `envs/` directories are
`.gitignore`'d — they hold per-deployment config and secrets — so a fresh
checkout does **not** contain `envs/<env>/`, and the scan has nothing to run
against.

You therefore point the job at a **runtime-visible config source** via
`scheduled_governance_config_source`: a Unity Catalog Volume (e.g.
`/Volumes/main/governance/genierails/prod`), workspace files, or a DBFS mount
holding that env's `auth.auto.tfvars`, `env.auto.tfvars`, `data_access/` and
(optionally) `generated/`. Before the scan, the wrapper copies that tree into
the checkout's env dir. This variable is **required when
`enable_scheduled_governance = true`** (enforced by a Terraform variable
validation), and the wrapper exits with clear guidance if the env config is
still missing at runtime.

Populate the source once from the machine that runs `make apply` (which already
has the env dir), for example:

```bash
databricks fs cp -r aws/envs/prod dbfs:/Volumes/main/governance/genierails/prod
```

## Disabled by default

The job is defined in
[`roots/workspace/scheduled_governance.tf`](../roots/workspace/scheduled_governance.tf)
and gated by `enable_scheduled_governance`, which defaults to `false`. Adding
the file changes **nothing** about `make apply` for existing environments —
the `databricks_job` resource has `count = 0` until you opt in.

## Enabling it

1. Copy the snippet from
   [`scheduled_governance.auto.tfvars.example`](../scheduled_governance.auto.tfvars.example)
   into the target environment's `env.auto.tfvars` (the tfvars file the
   workspace layer loads), for example `aws/envs/prod/env.auto.tfvars`:

   ```hcl
   enable_scheduled_governance = true
   scheduled_governance_env     = "prod"
   scheduled_governance_cloud   = "aws"          # or "azure"
   scheduled_governance_catalog = "prod_fin"     # optional; empty = auto-derive
   scheduled_governance_cron    = "0 0 6 * * ?"  # daily 06:00
   scheduled_governance_git_url = "https://github.com/<org>/<repo>"
   # Required: runtime-visible path holding the env config (envs/ is gitignored).
   scheduled_governance_config_source = "/Volumes/main/governance/genierails/prod"
   scheduled_governance_notification_emails = ["governance-team@example.com"]
   ```

2. Apply the workspace layer:

   ```bash
   make apply ENV=prod
   ```

   Terraform creates one Databricks Job named
   `GenieRails steady-state governance (prod)` running on serverless compute,
   scheduled per your cron expression.

3. Confirm it in the workspace: **Workflows → Jobs**. The job id and URL are
   exposed as the `scheduled_governance_job_id` / `scheduled_governance_job_url`
   outputs.

## Configuration reference

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `enable_scheduled_governance` | `false` | Master opt-in. `false` = no job created. |
| `scheduled_governance_env` | `"prod"` | Environment the scan targets. |
| `scheduled_governance_cloud` | `"aws"` | Cloud subdir holding `envs/` (`aws`/`azure`). |
| `scheduled_governance_catalog` | `""` | Optional catalog threaded to `generate_abac.py --delta` (masking-UDF catalog derivation). Empty = auto-derive from the env's `uc_tables`. |
| `scheduled_governance_cron` | `"0 0 6 * * ?"` | Quartz cron cadence (daily 06:00). |
| `scheduled_governance_timezone` | `"UTC"` | Timezone the cron is evaluated in. |
| `scheduled_governance_git_url` | `""` | Repo the job checks out (code only). **Required when enabled.** |
| `scheduled_governance_config_source` | `""` | Runtime-visible path (UC Volume / workspace / DBFS) holding the env config; materialized into the checkout before scanning. **Required when enabled** (`envs/` is gitignored). |
| `scheduled_governance_git_branch` | `"main"` | Branch to check out. |
| `scheduled_governance_git_provider` | `"gitHub"` | Git provider for the Jobs Git source. |
| `scheduled_governance_serverless_client` | `"2"` | Serverless environment client version. |
| `scheduled_governance_dependencies` | `["python-hcl2", "databricks-sdk", "pyyaml", "requests"]` | PyPI packages installed into the serverless environment so the scripts can read config + query the workspace. Append your LLM provider package (e.g. `anthropic`) for the delta classify path. |
| `scheduled_governance_notification_emails` | `[]` | Emails notified on failure. |
| `scheduled_governance_job_name` | `""` | Override the generated job name. |

## Notes

- The scan re-scans **live** table schemas, so the job's service principal needs
  the same workspace + warehouse access the manual `make audit-schema ENV=prod`
  flow uses.
- The steady-state scripts read config with `python-hcl2` and query the
  workspace with `databricks-sdk`. Those are declared in the serverless
  environment (`scheduled_governance_dependencies`) so the audit actually audits
  — without them it would read no config and report no managed tables. If your
  delta step must classify new columns, append your LLM provider package (e.g.
  `anthropic`) to that list.
- `generate_delta` is constrained to existing governed keys/values — it
  classifies new columns and drops stale assignments; it does **not** perform a
  full regeneration. Review and deploy any config changes through your normal
  CI/apply flow.
- The job **reports and re-derives**; it does not auto-`apply` Terraform. Wiring
  the delta output back into a commit/apply pipeline is intentionally left to
  your existing CI (see [cicd.md](cicd.md)).
