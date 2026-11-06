# Ship a Genie agent from dev to prod

Databricks Data Classification finds the sensitive columns in your tables, like emails and card numbers. GenieRails turns those findings into masks and access rules for your Genie agent. You end up with a dev and a prod env where each group sees only what it should, proven before anyone new gets access.

The journey: set up dev, prove the rules in dev, copy the proven rules to prod, release prod, and keep it healthy. This is the happy path; each step links to its [details, edge cases and CI](DETAILS.md), and words are explained [at the end](#glossary).

<a id="how-it-works"></a>
## The whole flow

The whole flow at a glance; the steps below walk through each line.

```bash
# Dev: set it up and prove it
make setup ENV=dev REPO=<your-repo-url>   # creates dev's files (REPO= optional); offers to commit (y)
#   fill in the env + agent files setup copied from the templates (Dev step 1)
#   Catalog Explorer: turn on Data classification and review what it found (Dev step 2)
make generate ENV=dev       # drafts masks and access rules from the tags; offers to commit + push (y)
make rehearse ENV=dev       # checks, applies dev, proves the masks work; first time: asks y/N about account setup

# Prod: copy what dev proved, then release it (same for stg or any env)
make setup ENV=prod         # then set deployer_principal_id, catalog_map, group_map
make promote-to ENV=prod    # copies dev's proven rules to prod; offers to commit + push (y)
#   Catalog Explorer: the same review on the prod catalog
make release ENV=prod       # applies prod in rounds; shows each round's plan, asks y/N; ends at "Release complete"
make evidence ENV=prod      # writes a record of what is live
make maintain ENV=prod      # on a schedule, on the deployment machine: masks columns tagged later

# lost? make next ENV=<env>
```

Questions appear only at a terminal ([CI / scripted](#ci--scripted)); each one is listed [at the end](#what-each-command-asks).

<a id="prerequisites--gather-required-values"></a>
## Before you start

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws          # or: cd azure — every make command runs from the cloud folder
```

- Have an empty repo you own ready for dev step 1's `make setup ENV=dev REPO=<your-repo-url>`.
- No repo of your own yet? Leave `REPO=` off; everything then stays in local git.
- More: [your repo and remotes](DETAILS.md#your-repo-and-remotes).

| You need | Notes |
|---|---|
| The [prerequisites](../../docs/prerequisites.md) | Tools, a deploying service principal (a non-human account that runs the deploys: `make bootstrap-sp`), groups synced from your identity provider |
| Your Genie **Agent ID** | Genie UI → agent → **Configure** → *About this agent*. *No agent yet?* Use the [sample environment](SAMPLE_ENV.md) |
| Who may run the agent | Its `acl_groups`: the groups that get Genie `CAN_RUN`, chosen from your access-tier groups |
| Deploying SP application ID | Each env's `databricks_client_id` (`auth.auto.tfvars`), also set in the env file as `deployer_principal_id` (not secret) |
| Access-tier groups | Your existing groups, most- to least-privileged, e.g. `payments_ops` sees raw values, `viewers` masked ones. Order matters: a tier listed above a masked tier also gets the agent and sees those columns unmasked |
| Dev and prod catalog names | e.g. `dev_finance` and `prod_finance` |

<a id="phase-0--dev-set-up"></a>
<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Dev

In dev you set up, find the sensitive columns, draft the rules and prove they work.

**1. Set up.** Create dev's files and save them as the first version in your repo.

```bash
make setup ENV=dev REPO=<your-repo-url>
```
- `REPO=` (optional) makes this clone yours: `origin` becomes your repo, where teammates can review your changes. Without it, everything stays in local git.
- Setup copies this walkthrough's templates into dev: [`env.auto.tfvars.example`](env.auto.tfvars.example) to `envs/dev/env.auto.tfvars` and [`agent.auto.tfvars.example`](agent.auto.tfvars.example) to `envs/dev/agents/my_agent.auto.tfvars`. It copies only files that don't exist yet, so it never overwrites your edits and a re-run changes nothing.

<a id="commit-it"></a>
**Then it offers to commit.** GenieRails previews the `.gitignore`, placeholder env defaults and agent file it created and asks `Commit these changes and push them to origin/main? [y/N]`. Answer `y`.

*What a commit is:* a saved snapshot of these files, with an ID. GenieRails wants one because:
- What you test in dev is exactly what reaches prod: prod releases only a commit that passed in dev.
- Teammates can review a change before it ships.
- Git keeps every version, so you can see what changed and undo it later.

Credentials (`auth.auto.tfvars`) are never committed. The edits you make next are committed by `make generate`'s own offer; later steps that write files offer the same way.

Now fill in the files:
- Fill in `envs/dev/auth.auto.tfvars` (service principal, workspace). In `envs/dev/env.auto.tfvars`, replace the deployer and group placeholders:
  ```hcl
  deployer_principal_id = "<the dev auth.auto.tfvars databricks_client_id>"
  access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
  ```
- **Agents (preferred):** rename setup's `my_agent.auto.tfvars` to your agent. Set its ID and `acl_groups` (from `access_tier_groups`):
  ```hcl
  genie_space_id = "01ef0000000000000000000000000000"   # Genie UI > agent > Configure > About this agent
  acl_groups     = ["regional_analysts", "viewers"]      # who gets CAN_RUN on the agent; [] means nobody
  ```

More: [more agents, inline `genie_spaces`, the `acl_groups` rules](DETAILS.md#dev-step-1-set-up), [the commit offer](DETAILS.md#dev-step-1-the-commit-offer).

<a id="dev-classify"></a>
**2. Review classification (in Catalog Explorer).** Let Databricks find and tag dev's sensitive columns.

- In **Catalog Explorer**, open the dev catalog → **Details** → **Data classification**: turn it on.
- When the scan finishes (the first one can take up to about a day), review the detections, exclude false positives, and turn on auto-tagging.
- Wait until the columns show `class.*` tags: check a table's **Columns** tab in Catalog Explorer.

*Why this review matters:* each tag says "this column is sensitive", and GenieRails builds the masks from these tags. Every tagged column the agent can reach needs a decision: a mask, or, if the detection is wrong, an exclusion here. Until it has one, GenieRails opens no new access, so nothing can leak by accident. More: [why there's no "raw for everyone" switch](DETAILS.md#why-no-unmasked-sensitive-column).

**3. Generate.** Draft the rules from the tags, so you review rules instead of writing them.

```bash
make generate ENV=dev
```
- Review the `table_readers` it proposes in `envs/dev/env.auto.tfvars` (who may read each table) and the drafted rules in `envs/dev/generated/`.
- No tags yet? It stops and says to wait; re-run it later.
- It then offers to commit and push `.gitignore` and `envs/dev/`. Answer `y`.

To change a mask, edit `envs/dev/env.auto.tfvars` and re-run `make generate ENV=dev`. In the template's default mode a re-run replaces hand edits to the masks in `envs/dev/generated/`. More: [what a re-run keeps](DETAILS.md#dev-step-3-generate).

**4. Rehearse.** Apply the rules to dev and prove the masks work.

*Why prove it in dev first:* a mistake in dev is cheap. Rehearse applies the real rules, then queries as each group to prove the masks hide what they should. Prod releases only a commit that passed here.

From a clean checkout at your pushed `origin/main`, in a terminal:
```bash
make rehearse ENV=dev
```
- It first runs `make check ENV=dev`. If a check fails, it stops before it takes the lock, writes anything or reads live dev.
- A first deploy shows the shared account plan and asks `Approve and apply this account manifest? [y/N]`. That plan is the account-wide setup all envs share, e.g. the masks' tag policy. `y` applies exactly what it showed; answer `y`.
- It applies dev, in phases on a first deploy: masks before access. Then it runs `verify-access` as its last gate (`_verify-access-gate`).

**Pass looks like:** `ALL EFFECTIVE`: each test tier sees masked values, the authorized tier sees raw ones. To re-run only that proof later: `make verify-access ENV=dev`.

More: [the account layer, phases, row-pairing keys](DETAILS.md#dev-step-4-rehearse).

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<a id="phase-3--prod-scan-real-data"></a>
<a id="phase-4--prod-release-and-verify"></a>
## Prod

In prod you don't write rules again: you copy the ones dev proved, then release them.

**1. Set up.** Create prod's files.

```bash
make setup ENV=prod
```
- Fill in `envs/prod/auth.auto.tfvars`. Setup makes no agent file for prod: `promote-to` writes prod's.
- In `envs/prod/env.auto.tfvars`, replace the `deployer_principal_id` placeholder and the seeded `catalog_map` line, and set the rest:
  ```hcl
  catalog_map = { "dev_finance" = "prod_finance" }   # dev catalog = prod catalog
  sql_warehouse_id = "<existing-prod-warehouse-id>"   # or "" to create a new serverless warehouse
  group_map   = { "payments_ops" = "prod_payments_ops" }   # dev group = prod group, only where they differ
  ```
- Leave the `access_tier_groups` placeholders: the next step writes prod's tiers from dev's, through `group_map`.

**2. Copy the proven rules.** `promote-to` copies the exact rules dev tested, renamed to prod's catalogs and groups.

```bash
make promote-to ENV=prod
```
- It then offers to commit and push `.gitignore`, `envs/prod/` and `envs/account/`: answer `y`.

More: [reusing dev's service principal, `catalog_map` rules, what `promote-to` copies](DETAILS.md#prod-steps-1-2-set-up-and-copy-the-rules).

**3. Review classification (in Catalog Explorer).** Do the same review as [dev step 2](#dev-classify), on the prod catalog.

Prod's real data may hold sensitive types dev never had.

**4. Release.** Apply the rules to prod, one approved round at a time.

One command, at a terminal on the deployment machine:
```bash
make release ENV=prod
```

*What a manifest is:* the exact list of changes this round will make, saved before anything happens. Each round shows you its manifest and asks `Approve and release manifest <id> ...? [y/N]`.
- `y` applies exactly that manifest. If anything changed since it was planned, it refuses.
- Anything else applies nothing and stops.

*Why several rounds:* a first release goes masks first, access last: the masks, then the column tags, then read access, then access to the agent. Each round is proven before the next opens more. Answer `y` each round until it prints `=== Release complete (prod) ===`.

- Before round 1, if the account plan has changes, it asks the same account question as rehearse.
- It reuses dev's rehearsal; it never rehearses dev. If it says `no passing rehearsal of <commit>`, run `make rehearse ENV=dev` first.

**Expect owner and admin reader refusals on the first round.** Owners and admins usually can read prod's tables outside your tiers. GenieRails doesn't manage that access, so you accept it in writing: an ack, one line in `envs/prod/ack.txt` saying "I reviewed this access and accept it". Add each printed `prod:reader:<table>:<principal>` key you accept, e.g.:
```text
# reviewed: owner group of prod_finance
prod:reader:prod_finance.payments.transactions:finance_admins
```
It's your sign-off, never a `y/N`, so no command commits it: commit and push it yourself, then run `make release ENV=prod` again.

**Pass looks like:** `ALL EFFECTIVE` (from its `verify-access` gate) and "Release complete". To re-run only that proof later: `make verify-access ENV=prod`.

More: [each round, the account layer, rehearsal reuse, reader acks](DETAILS.md#prod-step-4-release).

**If it stops on coverage or drift**, prod found a sensitive tag your rules don't cover. Nothing new was granted; the message prints the fix. More: [both fixes](DETAILS.md#if-prod-stops-on-coverage-or-drift).

**5. Record evidence.** Write a record of what is live in prod, for auditors.

```bash
make evidence ENV=prod
```
- It writes a JSON record and Markdown summary to `.genierails-state/prod/evidence/` (git-ignored).
- Archive the record, e.g. with the change ticket: the next run replaces it.

More: [warehouse, offline mode, state folder](DETAILS.md#prod-step-5-record-evidence).

<a id="phase-5--prod-maintain-coverage"></a>
## Keep it healthy

New sensitive columns appear over time: `maintain` catches columns tagged after the release and masks them.

```bash
make maintain ENV=prod      # on a schedule, on the deployment machine: masks columns tagged later
```

- Schedule it (e.g. with cron) on the [deployment machine](../../docs/deployment-machine.md).
- In a deterministic env it runs only from the last released commit, on a clean checkout.
- It never grants access past a passing coverage check and never changes the Genie agent. If it stops, it prints what to do.

More: [what maintain does](DETAILS.md#keep-it-healthy).

<a id="later-changes"></a>
## Later changes

Every later change goes through a pull request (PR) into `main`, with `make check` as its check. Commit by hand only what no command offers: `envs/prod/ack.txt`, edits on a branch for a PR, and anything you answered `n` to. More: [commits and PRs](DETAILS.md#commit-it), [Pipelines](../../docs/pipelines.md), and the [agents](../../docs/playbooks/agents.md), [governance](../../docs/playbooks/governance.md) and [one-team](../../docs/playbooks/one-team.md) playbooks.

<a id="ci--scripted"></a>
## CI / scripted

Off a terminal (a pipeline, CI, piped output) nothing asks, and no flag or variable answers an approval `y` for you. Pipelines and two-person approval use the low-level steps: [CI / scripted](DETAILS.md#ci--scripted).

## Reference

<a id="what-each-command-asks"></a>
### What each command asks

`evidence`, `maintain`, `next` and `status` ask nothing. Every detail: [the full table](DETAILS.md#what-each-command-does-in-full).

| Command | What it asks | What your `y` approves |
|---|---|---|
| `make setup ENV=dev REPO=<url>` | `Commit these changes and push them to origin/main? [y/N]` (no `origin`: `... and keep them local (no origin remote)? [y/N]`) | Save these files as a version in your repo |
| `make generate ENV=dev` | The same commit question | Same: save this step's files |
| `make rehearse ENV=dev` | `Approve and apply this account manifest? [y/N]`, only when the account plan has changes | The account-wide setup shown, e.g. the masks' tag policy |
| `make setup ENV=prod` | The same commit question | Same: save this step's files |
| `make promote-to ENV=prod` | The same commit question | Same: save this step's files |
| `make release ENV=prod` | `Approve and apply this account manifest? [y/N]` first, only when the account plan has changes; then `Approve and release manifest <id> (commit <sha>, round <n> of at most 8)? [y/N]`, once per round | Account: as above. Each round: exactly that round's prod changes |

<a id="glossary"></a>
### Words used on this page

All other terms: [full glossary](REFERENCE.md#glossary).

| Word | In plain words |
|---|---|
| **env** | One place you deploy to, e.g. `dev` or `prod`, with its own `envs/<env>/` folder |
| **tier** (access-tier group) | An existing group whose users see data at one level, e.g. `payments_ops` raw, `viewers` masked |
| **classification tag** (`class.*`) | The tag Data Classification puts on a sensitive column, e.g. `class.email_address` |
| **treatment / mask** | How a column is hidden from a tier, e.g. a card shown as `****-****-****-4464` |
| **commit** | A saved snapshot of your files, with an ID ([dev step 1](#commit-it)) |
| **rehearse** | The full test in dev: apply the rules, then prove each tier sees what it should |
| **manifest** | The exact list of changes a step will make, saved before anything happens; you approve it with `y` |
| **ack** (`ack.txt`) | A committed line saying "I reviewed this access and accept it" |
