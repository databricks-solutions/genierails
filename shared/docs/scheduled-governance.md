# Scheduled Governance Checks

The opt-in scheduled job is read-only by default. On serverless compute it runs
the schema-drift audit (untagged sensitive-looking columns and stale
assignments) and the rulebook audit (live production tags with no covering
policy or mask):

```bash
python shared/scripts/run_scheduled_governance.py \
  --env-dir aws/envs/prod --step check \
  --config-source /Volumes/main/governance/genierails/envs
```

The job does not run make, Terraform, derivation, or apply, and it does not
write to the workspace. Existing serverless dependencies are sufficient. The
config source is required because `envs/` is gitignored; it must be a
runtime-visible envs root containing both `account/` and the target environment
(for example `prod/`). This gives the rulebook audit the promoted account
policies without relying on local Terraform state.

On findings, the run fails and its output explains what was found. Open the run
from the failure notification, then run this from the authoritative GenieRails
checkout:

```bash
make maintain ENV=prod
```

Enable the job with `enable_scheduled_governance = true`, its Git URL and config
source, then `make apply ENV=prod`. Schedule, timezone, Git branch/provider,
dependencies, notifications, and job name use the existing
`scheduled_governance_*` variables.

## Legacy mode

Set `scheduled_governance_mode = "legacy"` to retain the explicit legacy
`audit -> generate-delta -> coverage` flow. Its model-based delta step may
rewrite files in the ephemeral checkout but never applies them. The runner also
retains `--step all|audit|delta|coverage` for existing callers;
`--auth-file` and `--catalog` are legacy-only.

## Upgrading

The mode defaults to `"check"`. Existing jobs therefore become less mutating:
they stop running legacy `generate-delta` and perform only the two read-only
audits. Set the legacy mode explicitly only if that old checkout-local rewrite
is still required.
