# Ship a Genie agent from dev to prod

Take a Genie agent you've curated in **dev** and ship it to **prod** with its sensitive columns masked. Databricks Data Classification finds the sensitive columns; GenieRails turns that into masks and access rules as code. No new business access is granted until every classified sensitive column the agent can reach is covered by a mask policy, checked against freshly read tags.

## The whole flow

```bash
# dev
make setup ENV=dev          # copy the templates; set deployer_principal_id and groups; per agent, its ID and acl_groups
#   Catalog Explorer: turn on Data classification, review detections, enable auto-tagging
make generate ENV=dev       # imports the agent, finds its tables, drafts the rules
make rehearse ENV=dev       # applies in dev and proves the masks work

# prod (same for stg or any env)
make promote-to ENV=prod    # copies the reviewed rules (set catalog_map first)
#   Catalog Explorer: same classification review on the prod catalog
make release-plan ENV=prod  # plans prod and writes the manifest to approve
make release ENV=prod MANIFEST=<manifest>   # applies exactly the approved plan, proves the masks
make maintain ENV=prod      # run on a schedule

# commit
make setup ENV=prod GITIGNORE=customer   # then git add .gitignore envs/ and open the first PR
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
| Who may run the agent | The groups that get Genie `CAN_RUN` on it (its `acl_groups`), chosen from your access-tier groups. Required: `[]` means nobody |
| Deploying SP application ID | The `databricks_client_id` from each env's `auth.auto.tfvars`; it is committed as `deployer_principal_id` (not secret) |
| Access-tier groups | Your existing groups, most- to least-privileged, e.g. `payments_ops`, `regional_analysts`, `viewers`. Order matters: a tier listed above a masked tier also gets the agent and sees those columns unmasked |
| Dev and prod catalog names | e.g. `dev_finance` and `prod_finance` |

<a id="phase-0--dev-set-up"></a>
<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Dev

**1. Set up.**
```bash
make setup ENV=dev
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
mkdir -p envs/dev/agents
cp ../shared/examples/dev_to_prod/agent.auto.tfvars.example "envs/dev/agents/Finance Analytics.auto.tfvars"
```
Fill in `envs/dev/auth.auto.tfvars` (service principal and workspace). The template starts new champions with `governance_mode = "deterministic"`; existing env files are not changed. In `envs/dev/env.auto.tfvars`, replace the deployer and group placeholders:
```hcl
deployer_principal_id = "<the dev auth.auto.tfvars databricks_client_id>"
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
```
Each agent has its own file in `envs/dev/agents/`; the file name is the agent's name. In it, set the agent's ID and who may run it:
```hcl
genie_space_id = "01ef0000000000000000000000000000"   # Genie UI > agent > Configure > About this agent
acl_groups     = ["regional_analysts", "viewers"]      # who gets CAN_RUN on the agent; [] means nobody
```
`acl_groups` is required: `make generate` stops while an agent has none. It grants only `CAN_RUN` on the agent. `SELECT` on the agent's tables comes only from `table_readers` in `env.auto.tfvars`, which `make generate` proposes for you (filled with `acl_groups`) for each table nothing covers yet. Every `acl_groups` group must be able to read every table of the agent, so pick them from `access_tier_groups` (only tier groups and `raw_exempt_principals` can be readers); otherwise `generate`, `rehearse` and `release` stop and name the table and group.

<a id="dev-classify"></a>
**2. Classify (in the UI).** In **Catalog Explorer**, open the dev catalog → **Details** → **Data classification**: turn it on. When the scan finishes (the first one can take up to about a day), review the detections, exclude false positives, and turn on auto-tagging. Then wait until the columns show `class.*` tags such as `class.email_address`: open a table in Catalog Explorer and check its **Columns** tab.

**3. Generate.**
```bash
make generate ENV=dev
```
It captures each new agent to `envs/dev/agents/<agent>.space.json` and proposes its `table_readers`. Review those and the drafted rules in `envs/dev/generated/`, and edit the rules if you need to. If the tags aren't there yet, it stops and tells you to wait; re-run it later. Re-runs keep whatever is already in `envs/dev/generated/`, including your edits, and only add rules for new columns.

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
Fill in `envs/prod/auth.auto.tfvars`, replace its `deployer_principal_id` and `access_tier_groups` placeholders, and set:
```hcl
catalog_map = { "dev_finance" = "prod_finance" }   # dev catalog = prod catalog
```
Then:
```bash
make promote-to ENV=prod
```
Using the sample environment? Load its tables into prod first (`--skip-agent`, see [SAMPLE_ENV.md](SAMPLE_ENV.md)).

**2. Classify (in the UI).** Same as dev, on the prod catalog. Prod's real data may surface sensitive types dev never had.

**3. Release.** Plan, review the manifest, then apply exactly what was approved:
```bash
make release-plan ENV=prod
make release ENV=prod MANIFEST=<manifest printed by release-plan>
```
**Pass looks like:** `ALL EFFECTIVE` and "Release complete". Business users can now open the agent and get masked answers.

**If it stops on coverage or drift**, prod found a sensitive tag your rules don't cover. Nothing new was granted. The message prints both fixes with exact commands:
- **Reuse an existing mask** (the common case): `make scaffold-treatments ENV=prod TREATMENT=<name>` adds the class to `tag_treatments` in `envs/dev/env.auto.tfvars` (`shared/` is unchanged). Commit it, then `make generate ENV=dev`, `make rehearse ENV=dev`, `make promote-to ENV=prod`, `make release-plan ENV=prod` and `make release ENV=prod MANIFEST=<manifest>`.
- **A new kind of mask**: draft and test it with the dev-only helpers (`make draft-custom-mask` / `make test-custom-mask`, see the [governance playbook](../../docs/playbooks/governance.md)). Applying a custom mask in governance (`make adopt-custom-mask`) is not on `main` yet. The bare `make scaffold-treatments` stub and `materialize-treatment` are legacy-mode only ([legacy mode](../../docs/legacy-mode.md)).

Details: [Fixing a coverage gap found in prod](REFERENCE.md#fixing-a-coverage-gap-found-in-prod).

<a id="phase-5--prod-maintain-coverage"></a>
## Keep it safe

```bash
make maintain ENV=prod      # schedule it (cron or CI)
```
It masks newly tagged columns, never grants access past a passing coverage check, and never changes the Genie agent. If it stops, it prints what to do: usually review a new detection in Catalog Explorer, or add a missing rule in dev and re-promote.

## Commit it

Put it in your own repo (create an empty one first) and open the first PR. From `aws/` (or `azure/`):
```bash
make setup ENV=prod GITIGNORE=customer   # once: commits envs/, ignores credentials, state and runtime files
git remote rename origin genierails      # keep GenieRails for upgrades
git remote add origin <your-repo-url>
git push -u origin main
git switch -c first-agent
git add .gitignore envs/                 # plus any ../shared/ file a coverage fix changed
git status                               # pass looks like: no auth.auto.tfvars, *.tfstate or .genie_* files
git commit -m "Govern the first agent in dev and prod"
git push -u origin first-agent
```
Open a pull request from `first-agent` into `main`. From then on every change goes through a PR: see [Pipelines](../../docs/pipelines.md) to set up CI, and the [agents](../../docs/playbooks/agents.md), [governance](../../docs/playbooks/governance.md) or [one-team](../../docs/playbooks/one-team.md) playbook for day-2 changes.
