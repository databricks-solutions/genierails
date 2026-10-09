# Ship a Genie agent from dev to prod

Take a Genie agent you've curated in **dev** and ship it to **prod** with its sensitive columns masked. Databricks Data Classification finds the sensitive columns; GenieRails turns that into masks and access rules as code. No new business access is granted until every classified sensitive column the agent can reach is covered by a mask policy, checked against freshly read tags.

## The whole flow

```bash
# dev
make setup ENV=dev          # then copy the template; set genie_space_id and access_tier_groups
#   Catalog Explorer: turn on Data classification, review detections, enable auto-tagging
make generate ENV=dev       # imports the agent, finds its tables, drafts the rules
make rehearse ENV=dev       # applies in dev and proves the masks work

# prod (same for stg or any env)
make promote-to ENV=prod    # copies the reviewed rules (set catalog_map first)
#   Catalog Explorer: same classification review on the prod catalog
make release ENV=prod       # applies in prod and proves the masks work
make maintain ENV=prod      # run on a schedule
```

<a id="prerequisites--gather-required-values"></a>
## Before you start

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws           # or: cd genierails/azure — every make command runs from here
```

| You need | Notes |
|---|---|
| The [prerequisites](../../docs/prerequisites.md) | Tools, a deploying service principal (`make bootstrap-sp`), IdP-synced groups |
| Your Genie **Agent ID** | Genie UI → open the agent → **Configure** → *About this agent*. *No agent yet?* Use the [sample environment](SAMPLE_ENV.md) |
| Access-tier groups | Your existing groups, most- to least-privileged, e.g. `payments_ops`, `regional_analysts`, `viewers` |
| Dev and prod catalog names | e.g. `dev_finance` and `prod_finance` |

You don't choose a verification key: GenieRails picks and proves one per table.

<a id="phase-0--dev-set-up"></a>
<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Dev

**1. Set up.**
```bash
make setup ENV=dev
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
```
Fill in `envs/dev/auth.auto.tfvars` (service principal and workspace). In `envs/dev/env.auto.tfvars`, replace `<your-genie-space-id>` with your Agent ID and set:
```hcl
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
```

<a id="dev-classify"></a>
**2. Classify (in the UI).** In **Catalog Explorer**, open the dev catalog → **Details** → **Data classification**: turn it on. When the scan finishes, review the detections, exclude false positives, and turn on auto-tagging. Wait for the `class.*` tags to appear (the first scan can take up to about a day).

**3. Generate.**
```bash
make generate ENV=dev
```
Review the drafted rules in `envs/dev/generated/`. If the tags aren't there yet, it stops and tells you to wait; re-run it later. Re-runs keep the rules you've reviewed.

**4. Rehearse.**
```bash
make rehearse ENV=dev
```
**Pass looks like:** `ALL EFFECTIVE`: each test tier sees masked values, the authorized tier sees raw ones.

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<a id="phase-3--prod-scan-real-data"></a>
<a id="phase-4--prod-release-and-verify"></a>
## Prod

**1. Set up and promote.**
```bash
make setup ENV=prod
```
Fill in `envs/prod/auth.auto.tfvars`, and in `envs/prod/env.auto.tfvars` set:
```hcl
catalog_map = { "dev_finance" = "prod_finance" }   # dev catalog = prod catalog
```
Then:
```bash
make promote-to ENV=prod
```
Using the sample environment? Load its tables into prod first (`--skip-agent`, see [SAMPLE_ENV.md](SAMPLE_ENV.md)).

**2. Classify (in the UI).** Same as dev, on the prod catalog. Prod's real data may surface sensitive types dev never had.

**3. Release.**
```bash
make release ENV=prod
```
**Pass looks like:** `ALL EFFECTIVE` and "Release complete". Business users can now open the agent and get masked answers. (They also need `CAN_USE` on the agent's SQL warehouse, which GenieRails doesn't grant.)

**If it stops on coverage or drift**, prod found a sensitive tag your rules don't cover. Nothing new was granted. Fix it in dev, never by hand in prod:
1. `make scaffold-treatments ENV=prod` names the uncovered tag and adds a suggested mask to `../shared/treatment_config.json`. Review it; it's a tracked file, so it goes in the same PR as step 2.
2. Add the rule to dev: tag a matching dev column, then `make generate ENV=dev` ([no matching dev column?](REFERENCE.md#step-details)).
3. `make rehearse ENV=dev`, then `make promote-to ENV=prod` and `make release ENV=prod`.

<a id="phase-5--prod-maintain-coverage"></a>
## Keep it safe

```bash
make maintain ENV=prod      # schedule it (cron or CI)
```
It masks newly tagged columns, never grants access past a passing coverage check, and never changes the Genie agent. If it stops, it prints what to do: usually review a new detection in Catalog Explorer, or add a missing rule in dev and re-promote.

## If something stops

| Message | What to do |
|---|---|
| No `class.*` tags yet | Finish the Catalog Explorer review and auto-tagging, wait, re-run |
| Coverage check failed / rulebook drift | See "If it stops on coverage or drift" under Prod step 3 |
| No provable row-pairing key for a table | Set `verify_key_columns = { "<cat.sch.tbl>" = "<column>" }` ([how keys are picked](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key)) |
| Genie agent ID file missing | Follow the printed recovery steps; never re-create the agent by hand |
| Env is locked | Another `release`/`maintain` is running; wait for it |

To change who has access in prod, edit `envs/prod/env.auto.tfvars` in a PR and let the pipeline apply it.

<a id="reference--commands-concepts-and-glossary"></a>
**More:** [REFERENCE.md](REFERENCE.md) (every command, what each step does, how it works, glossary) · [SAMPLE_ENV.md](SAMPLE_ENV.md) · [Import an existing agent](../../docs/import-genie-agent-from-ui.md)
