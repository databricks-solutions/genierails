# Scheduled Steady-State Governance

The opt-in scheduled job runs the native-first dev-to-prod steady state:

```bash
python shared/scripts/run_scheduled_governance.py \
  --env-dir aws/envs/prod --step maintain \
  --config-source /Volumes/main/governance/genierails/prod
```

`maintain` is the wrapper's default and delegates directly to
`make maintain ENV=prod`. It therefore uses the same per-environment lock and
the same ordered flow: audit schema, derive assignments from native `class.*`
tags, run the coverage gate, validate generated configuration, apply governance,
audit the rulebook, and renew the certification receipt. It never changes
`business_access_enabled` or applies Genie. A non-zero make exit fails the job
and preserves maintain's remediation hint in the job output and failure
notification context.

## Enable the job

The job in `roots/workspace/scheduled_governance.tf` is disabled by default.
Set these workspace-layer variables and run `make apply ENV=prod`:

```hcl
enable_scheduled_governance = true
scheduled_governance_env = "prod"
scheduled_governance_cloud = "aws" # or "azure"
scheduled_governance_git_url = "https://github.com/<org>/<repo>"
scheduled_governance_config_source = "/Volumes/main/governance/genierails/prod"
scheduled_governance_notification_emails = ["governance-team@example.com"]
```

The config source is required because `envs/` is gitignored. It must contain
the target environment's configuration and be visible to the job runtime. The
wrapper copies it into the Git checkout before invoking make. The service
principal needs the same workspace, warehouse, and governance privileges as a
manual `make maintain` run. Schedule, timezone, Git branch/provider, serverless
dependencies, notifications, and job name remain configurable through the
existing `scheduled_governance_*` variables.

## Legacy mode

Existing callers can explicitly use `--step all` for the legacy
`audit -> generate-delta -> coverage` flow, or select `audit`, `delta`, or
`coverage` individually. This model-based `[Legacy]` path remains unchanged;
`--auth-file` and `--catalog` apply only to its delta step. The shipped job uses
`--step maintain`.
