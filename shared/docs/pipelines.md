# Pipelines

How to set up the repo and the GitHub Actions or Azure DevOps pipelines that
run GenieRails. People run the authoring commands on their own machines;
pipelines run the rest. Design: [section 17](design/deterministic-governance.md#17-where-each-command-runs).

## The whole setup

```bash
# from aws/ (or azure/), as an account admin, once per env
make bootstrap-sp ACCOUNT_PROFILE=<profile> ACCOUNT_ID=<id> WORKSPACE_ID=<dev-ws> TARGET_CATALOG=<dev-catalog> YES=1
#   commit its application ID as deployer_principal_id in envs/dev/env.auto.tfvars
make setup ENV=dev                             # in your repo: writes the .gitignore that commits envs/ and ignores runtime files
make bootstrap-pipelines ENV=dev  CI=github ACCOUNT_ID=<id> WORKSPACE_ID=<dev-ws>  REPO=<owner>/<name> PLAN=1
make bootstrap-pipelines ENV=dev  CI=github ACCOUNT_ID=<id> WORKSPACE_ID=<dev-ws>  REPO=<owner>/<name>
make bootstrap-pipelines ENV=prod CI=github ACCOUNT_ID=<id> WORKSPACE_ID=<prod-ws> REPO=<owner>/<name> WAREHOUSE_ID=<id>
#   apply the printed repo variables, environments, branch protection and CODEOWNERS
#   copy the four samples from <cloud>/.github/workflows/ to the repo's .github/workflows/
#   commit envs/dev/remote_state.hcl and envs/account/remote_state.hcl (dev pipeline only)
```

1. **Deployer SP per env.** `make bootstrap-sp` ([prerequisites](prerequisites.md)).
   Commit its application ID as `deployer_principal_id` in that env's
   `env.auto.tfvars` ([access.md](access.md#the-deployer-sp-deployer_principal_id)).
2. **Git tracking.** [What to commit](#what-to-commit).
3. **Pipeline identities.** `make bootstrap-pipelines`, `PLAN=1` first ([below](#setting-up-the-pipelines)).
4. **Repo settings.** Apply what it prints: [branch protection and CODEOWNERS](#branch-protection-and-approvals).
5. **Samples.** Copy the ones you use ([stage table](#pipeline-samples-where-each-command-runs)).
6. **Dev remote state** (only to run stage 3 in CI): [dev remote state](#state-location-and-dev-remote-state).

**Pass looks like:** a re-run of `make bootstrap-pipelines` reports every item
`UNCHANGED`, and the PR check on your first PR ends with
`=== make check ENV=dev: passed ===`.

**If it stops:** `bootstrap-pipelines` refuses before any write and names what's
missing: not an account admin, no authority over a catalog, no `CAN_MANAGE` on
an agent or warehouse, a `deployer_principal_id` not in the account, or a
read-only SP that already holds more than it should. Fix that and re-run.

## Who runs what

| Stage | Where | Command | Changes Databricks? |
|---|---|---|---|
| 1. Author | a person's machine | `make capture ENV=dev SPACE="<agent>"`, `make generate ENV=dev`, `make scaffold-treatments`, `make promote-to ENV=prod`; commit, open a PR | no |
| 2. PR check | CI, every PR | `make check ENV=dev`; also `make check ENV=prod` when the PR touches `envs/prod/**` or `shared/**` | no |
| 3. Dev rehearsal | dev pipeline, each merge to `main`, in order | `make rehearse ENV=dev` | dev |
| 4. Prod release | deployment machine in v1, after stage 3 passes on the commit | `make release-plan ENV=prod`, approve its manifest, `make release ENV=prod` ([release gating](#release-gating-deterministic-envs)) | prod |
| 5. Keep protected | deployment machine in v1, on a schedule, at the last released commit | `make maintain ENV=prod` | prod, governance only |
| 6. Operations | deployment machine, one approval per operation | `make detach-agent`, `make account-plan` then `make apply-account`, migration phases | prod or account |

- **Stage 3 runs in CI once dev has remote state.** Until then the operator
  runs it. Stages 4 to 6 stay on the [deployment machine](deployment-machine.md)
  until prod remote state exists (design section 15).
- **Promotion is a person, not a pipeline.** `make promote-to ENV=prod` reads
  `promote_from` and `catalog_map` from `envs/prod/env.auto.tfvars`; commit its
  output in a PR. No pipeline writes to git.
- **Never run `make generate` in CI or prod.** Prod re-derives from its own tags.

Which commands each person runs: [agents](playbooks/agents.md),
[governance](playbooks/governance.md), [one team](playbooks/one-team.md).

## What to commit

GenieRails' own `.gitignore` ignores `envs/`. In your own repo, `make setup`
writes a `.gitignore` that commits `envs/`:

```bash
make setup ENV=dev
git status          # pass looks like: no auth.auto.tfvars, *.tfstate or .genie_* files
```

Commit `envs/<env>/env.auto.tfvars`, `envs/<env>/generated/`,
`envs/<env>/agents/`, `envs/<env>/ack.txt`, and `remote_state.hcl` where you use
it. Without `generated/`, `make check` skips the regeneration and coverage
checks. Never commit `auth.auto.tfvars`, state, locks, receipts or any dot-file
under `envs/`.

<a id="setting-up-git-tracking"></a>
**How the `.gitignore` block works.** In your own repo `make setup` writes a
GenieRails block into `<cloud>/.gitignore` (and prints it as a diff). It
re-includes `envs/` and ignores every runtime file: credentials, Terraform
state and plans, `.terraform/`, locks, receipts, Genie ID and adoption markers
(`.genie_*`), every dot-file under `envs/` (such as `generated/.runtime/` and
`data_access/.policy_generations/`), `classification.auto.tfvars`,
`discovered_uc_tables.auto.tfvars`, `ddl/_fetched.sql`, `.genierails-state/`
and, for each deterministic env, its layer files (applying commands re-derive
those).

`make setup` decides from the `origin` remote. If `origin` is
`github.com/databricks-solutions/genierails` (https or ssh), it is GenieRails'
own repo, which must never commit `envs/`: it writes nothing and only prints how
to opt in (as it does outside a git checkout). Any other checkout (your fork, an
internal mirror, no origin yet) gets the block, even if it keeps GenieRails as
an `upstream` remote for upgrades. An existing block is always refreshed. The
overrides: `GITIGNORE=customer` forces the write anywhere, `GITIGNORE=off`
leaves the file alone. If you contribute to GenieRails from a fork, always pass
`GITIGNORE=off` so setup leaves the upstream cloud `.gitignore` files unchanged.
Customer repositories may keep a GenieRails remote for upgrades; setup does not
print contributor-only advice in customer-facing output.

Only the lines between the block's markers are rewritten. The block names each
deterministic env, so re-run `make setup ENV=<env>` after adding an env or
changing `governance_mode`. Applying commands never write committed files, so a
release needs, and leaves, a clean checkout.

## Who owns which file

Each persona changes its own files (design [18.1](design/deterministic-governance.md#181-personas-and-the-files-each-owns)),
so CODEOWNERS can require only the owners of what changed:

| Persona | Files |
|---|---|
| Governance engineer | `envs/<env>/env.auto.tfvars`, `envs/<env>/generated/` (including `governed_tables.json`), `envs/<env>/ack.txt`, `shared/` |
| Data citizen | `envs/<env>/agents/<agent>.auto.tfvars` and `envs/<env>/agents/<agent>.space.json` |

- An agent's settings live only in its `agents/` files, so an agent-only PR
  never touches `env.auto.tfvars`.
- An agent owns its table list; governance approves the result. `capture`,
  `generate` and `promote-to` record each agent's tables in
  `generated/governed_tables.json`, so an agent change that adds or removes a
  table always changes a governance-owned file, and `make check` fails while
  that record is stale. The record grants nothing: `SELECT` comes only from
  `table_readers` ([access.md](access.md)).
- An agent kept inline in `env.auto.tfvars` with only a `genie_space_id` is
  recorded there too, under that ID, from the tables `make generate`
  discovered. The discovery file is git-ignored, so this committed record is
  what `make promote-to` and `make check` read: a fresh clone or CI promotes
  the same tables as your laptop.
- `make promote-to` writes agent output under `envs/<target>/agents/` and
  governance output under `envs/<target>/generated/`, and prints which kind it
  changed.
- Customer class mappings go in the env's `tag_treatments`, never `shared/`
  (GenieRails product code, replaced on upgrade).

**Agents still inline in `env.auto.tfvars`?** An env in the older layout keeps
them in `genie_spaces` and needs a governance approval for every agent change.
Move them once, in a PR:

```bash
make split-agent-files ENV=dev   # then each promote-to target, e.g. ENV=prod
make plan ENV=dev                # no change: Terraform gets the same agents
```

It moves each entry's `genie_space_id`, `acl_groups`, `uc_tables` and
`sql_warehouse_id` into `agents/<name>.auto.tfvars` and sets
`genie_spaces = []`. Every file records the existing Terraform `key`
separately from its display `name`, so an unnamed agent (keyed by ID), a
title containing filename-unsafe characters, or one of two titles that
sanitize alike (`a_b`, `a_b--<id|index>`) keeps the same resource address,
ID file (`.genie_space_id_<key>`) and display title. No state move or `moved`
block is needed. The file name is the title when it is a valid file name,
else the key; choose it with
`AGENT_FILE_NAMES='existing-key=my-file-name'` (comma-separate multiple
mappings). Never edit `key`; editing `name` only retitles the agent.

The command holds the env lock and refuses in CI. It refuses, writing nothing,
for a setting agent files don't hold (such as `declared_footprint`), a pending
`delete = true`, a key that isn't letters, digits, `_` and `-`, or an agent
ID in a promoted env (move it to `.genie_space_id_<key>` first). Migrate an
env and its promote-to targets together: `make promote-to ENV=prod` refuses
while prod still has an agent inline that dev keeps in a file. promote-to
carries the remapped display `name` (an unnamed dev agent takes its title
from `genie_space_id_to_name`); a prod agent split on its own is matched by
file name or title, never seeded twice.

## Branch protection and approvals

- Make the PR check a required status check and require branches to be up to
  date, so a PR is re-checked against the latest `main`. No direct pushes to
  `main`, including admins.
- **CODEOWNERS.** Start from
  [`shared/examples/CODEOWNERS.example`](../examples/CODEOWNERS.example) (or the
  one `bootstrap-pipelines` prints with `GOVERNANCE_OWNERS=` and
  `AGENT_OWNERS=`), copy it to `.github/CODEOWNERS` and turn on "Require review
  from Code Owners". Governance owns `/shared/` and every env file; data
  citizens own `/aws/envs/*/agents/` and `/azure/envs/*/agents/`, listed after
  the env-wide rules because the last match wins. `envs/*/ack.txt` must be
  governance-owned. An agent-only promotion then needs no governance approval.
- **Prod environment** (the samples use `production` on GitHub and
  `genierails-prod` on Azure DevOps): required reviewers from the governance
  code owners, no self-approval, `main` only.
- On GitHub, the prod release-plan job runs in the `production-plan`
  environment (`main` only, no reviewer), so the prod read-only SP trusts only
  that job and the `production-maintain` one, not every workflow on `main`.

## Setting up the pipelines

`make bootstrap-pipelines` sets up the Databricks side, one env at a time, and
prints everything to set on the repo side. Run it as an account admin, from
your cloud's folder, after `make bootstrap-sp` and after committing each env's
`deployer_principal_id` (commands in [the whole setup](#the-whole-setup)).
For Azure DevOps use `CI=azure-devops ADO_ORG=<org> ADO_ORG_ID=<org-guid> ADO_PROJECT=<project>`
instead of `REPO`. Pass `CI=` on the command line; a runner's `CI=true` is refused.

| It does | Details |
|---|---|
| Creates or reuses the env's **read-only SP** | `genierails-<env>-readonly` (`READONLY_SP_NAME=`), workspace `USER` access, no secret |
| Adds **OIDC federation** for each pipeline identity | read-only SP: the PR check, plus for prod the release-plan and maintain jobs (on GitHub, each under its own environment: `production-plan` and `production-maintain`, main only, no reviewer); dev deployer SP (`deployer_principal_id`): the dev rehearsal. The prod deployer is not federated while prod releases run on the deployment machine |
| Grants the **section 17 privileges**, nothing more | `USE CATALOG`, `USE SCHEMA`, `BROWSE` on the env's catalogs (default: the catalogs of its committed `uc_tables`; `CATALOGS=` overrides); read on `system.information_schema` (only when not already inherited); `CAN_VIEW` on the agents (dev: the committed IDs; prod: `GENIE_SPACE_IDS=`, from the release receipt); `CAN_USE` on `WAREHOUSE_ID` when given (prod maintain's drift audit needs one) |
| **Prints** the repo settings | repo variables (or the `genierails-ids` variable group), environments with required reviewers, branch protection, the Azure DevOps pipeline names the federation subjects use, and a `CODEOWNERS` that matches the [persona split](#who-owns-which-file) |

- **`PLAN=1`** runs the read-only preflight and prints the plan and the
  settings; it changes nothing.
- **It refuses before any write** when the caller can't make every change.
  Nothing was changed.
- **A re-run changes nothing** and reports each item `UNCHANGED`.
- **It never writes to the repo and never makes or prints a secret.** Every
  value it prints is an ID. Apply the printed settings yourself; the remote
  state roles (`DEV_STATE_*`) and service connection are yours to create.
- **It never revokes or downgrades, so it refuses instead** while the
  read-only SP is a workspace `ADMIN` or in the `admins` group, or effectively
  holds more than `CAN_VIEW` on an agent (`CAN_RUN`, `CAN_EDIT`, `CAN_MANAGE`)
  or more than `CAN_USE` on the warehouse (`CAN_MANAGE`, `IS_OWNER`):
  directly, through one of its groups (including `users`), or inherited. The
  message names where the grant comes from; remove it by hand and re-run.

## Pipeline samples: where each command runs

Each cloud wrapper ships one sample per stage, for GitHub Actions
(`<cloud>/.github/workflows/`) and Azure DevOps (`<cloud>/azure-pipelines/`).
GitHub only runs workflows from `.github/workflows/` at the repository root, so
copy the ones you use there; in Azure DevOps, create a pipeline from each file.
The samples run from `aws/` or `azure/`.

| Stage | Sample | Trigger | Runs in CI in v1 |
|---|---|---|---|
| 2. PR check | `pr-check.yml` | every PR to `main`; make it required and require up-to-date branches (Azure Repos: a build-validation policy that re-runs when the target branch changes) | `make check ENV=dev`; also `make check ENV=prod` when the PR touches `envs/prod/**` or `shared/**` (the prod release counts both) |
| 3. Dev rehearsal | `dev-rehearse.yml` | merge to `main` that touches the cloud dir or `shared/`; one run per commit, in order, never cancelled (GitHub: a queue job and no concurrency group; Azure DevOps: `batch: false`, `lockBehavior: sequential` and an Exclusive Lock check); a run stops only when a newer `main` commit changes the same paths, whose own run rehearses it, so a later README or pipeline-only commit never strands a rehearsal | `make check ENV=dev`, then `make rehearse ENV=dev` (once dev has remote state) |
| 4. Prod release | `prod-release.yml` | dev pipeline success on `main`, only when `envs/prod/**` or `shared/**` changed since the last successful prod release run (the commit GitHub or Azure DevOps records for it, `head_sha` or `sourceVersion`, never a run title; no such ancestor: always), so a change in an earlier skipped commit is never dropped (GitHub `workflow_run`; Azure DevOps pipeline-resource trigger); a protected environment with a required approval; no run is cancelled | `make check ENV=prod` on that commit, then the approval gate; the applying steps are marked `REQUIRES PROD REMOTE STATE` and only report the commit to release on the deployment machine |
| 5. Keep protected | `prod-maintain.yml` | a schedule (GitHub: its own concurrency group, never the release's) | `make audit-schema ENV=prod` (read-only drift audit) |

**Auth.** Every Databricks credential is a service principal through OIDC
workload identity federation (`github-oidc` or `azure-devops-oidc`): no PATs and
no client secrets. The samples write `auth.auto.tfvars` with IDs only and set
`DATABRICKS_AUTH_TYPE`. OIDC covers the pipelines only; the deployment machine
keeps its own `auth.auto.tfvars`.

**`make check` is read-only.** Each step runs in a scratch copy of the cloud dir
without credentials, Terraform state or runtime records; only the plan step
gets the env's auth files. It runs `validate`, a regeneration that fails if a
committed generated file or `governed_tables.json` differs (and, with
`ENV=prod`, `make promote-to ENV=prod CHECK=1`), `validate-generated`, the
offline coverage check, the mask-library tests and a **structural** plan (the
committed config against empty state, live tags not re-read). It is safe under
`CI=true`.

## CI guard

Applying targets (`apply`, `apply-governance`, `apply-genie`, `apply-account`,
`rehearse`, `release-plan`, `release`, `maintain`, `enable-classification`,
`ungovern`, `detach-agent`, `destroy*`, `import`, `migrate-state`,
`migrate-state-dir`, `policy-abandon`, `policy-rebind`) refuse when they
detect CI: `CI=true|1|yes`, or `TF_BUILD`, `JENKINS_URL`, `GITLAB_CI`,
`BUILDKITE` or `CIRCLECI` set (`policy-rebind` even with `GENIERAILS_ALLOW_CI_APPLY=1`). The sole exception is exactly
`make rehearse ENV=dev`, on `<cloud>/envs/dev` itself (not a promote-to
destination), and only when `envs/dev/remote_state.hcl` configures a locked
backend (S3 plus DynamoDB or `use_lockfile` on AWS; Azure Storage blob leases on
Azure). Recursive applies inside it pass only while they inherit that
rehearse's env lock. Legacy-mode dev is refused in CI.

`make check`, `plan`, `validate` and `audit-schema` are allowed. `sync-tags`,
`bootstrap-sp`, `bootstrap-pipelines` and `generate` are not guarded, so don't
run them from CI. `GENIERAILS_ALLOW_CI_APPLY=1` exists only for GenieRails' own
throwaway integration tests; never set it on a deployment job.

## State location and dev remote state

Where the deployment machine keeps state (`GENIERAILS_STATE_DIR`,
`make migrate-state-dir`): [deployment machine](deployment-machine.md#state-outside-the-checkout).

To run stage 3 in CI, opt dev into remote state with `envs/dev/remote_state.hcl`:

```hcl
# AWS
backend = "s3"
bucket = "company-genierails-state"
region = "ap-southeast-2"
dynamodb_table = "genierails-terraform-locks"   # or: use_lockfile = true (Terraform 1.10+)
key_prefix = "genierails"

# Azure uses instead (blob leases lock):
# backend = "azurerm"
# resource_group_name = "rg-genierails-state"
# storage_account_name = "genierailsstate"
# container_name = "tfstate"
```

- Each layer's state is the key `<key_prefix>/<env>/<layer>.tfstate`. Only
  non-secret settings are allowed; access keys, passwords and tokens are
  refused. Credentials come from the identity running Terraform (OIDC in the
  pipeline).
- The file is part of the dev fingerprint, so a backend change invalidates
  earlier receipts.
- Push an existing dev state to its key before committing the file (for
  example `terraform state push`), or the first plan sees an empty env.
- The dev pipeline also plans the account layer read-only (rehearse refuses a
  non-empty account plan), so commit `envs/account/remote_state.hcl` with the
  account's real state too. The deployment machine's `make account-plan` and
  `make apply-account` then use the same remote state.

**Receipts.** A receipt counts only in the store that wrote it. The dev
pipeline uploads its receipt as an artifact for the record, but `release-plan`
ignores a receipt copied in from elsewhere.

## Release gating (deterministic envs)

What each applying command checks for itself (design sections 10 and 17).
How the operator runs them: [deployment machine](deployment-machine.md#release-prod).

- **Rehearse receipts.** A passing `make rehearse ENV=dev` takes the dev lock
  and writes an append-only receipt for the commit: commit, clean-tree flag,
  the fingerprint of the code and of the dev and account inputs, the result,
  and the digest of what `make promote-to` produces for each env promoted from
  dev. When the fingerprint matches an earlier passing receipt, as for a
  prod-only PR, rehearse writes a receipt without re-applying.
- **`make release-plan ENV=prod`** refuses unless HEAD has a passing receipt
  of prod's source env (`promote_from`, dev by default) from a clean
  checkout, the checkout is still clean, HEAD is on freshly
  fetched `origin/main`, the fingerprint and the committed
  `envs/prod/generated` digest match the receipt, and HEAD isn't older than
  the last release. Then, under the prod lock, it re-derives prod from live
  tags into `envs/prod/generated/.runtime/` (ignored), keeps the policy names
  prod already deploys, runs the reader inventory and refuse-weakening against
  the deployed state (reconciling every `ack.txt` entry once), saves a
  Terraform plan for every root and writes **one immutable manifest**, which
  becomes prod's current manifest (a newer `release-plan` supersedes it). It
  changes nothing in Databricks. The account plan must be empty.
- **`make release ENV=prod`** releases the current manifest: it prints the
  manifest's path, commit and changes, applies only its saved plans, then
  verifies, and writes a release receipt. It refuses, and asks for a fresh
  `release-plan`, if there is no current manifest, it was already released,
  or the commit, a state serial, a saved plan, the effective config or live
  Unity Catalog changed. A pipeline passes `MANIFEST=<path>` to name the
  approved manifest explicitly; a superseded or already released one refuses.
- **`make account-plan` / `make apply-account`** is the account
  operation equivalent: `account-plan` refuses unless the checkout is clean and
  exactly current `origin/main`, saves one account plan and immutable manifest,
  and changes nothing live. The manifest records the saved plan hash, account
  input hashes and the tag-policy diff (additions and approved removals). `apply-account` chooses
  the single unreceipted manifest for `HEAD` (or accepts `MANIFEST=<path>` as an
  explicit override) and refuses in CI, from a dirty checkout, from any commit
  other than the manifest commit and the exact current `origin/main` tip, if
  the account state, manifest, saved plan, input hashes or tag-policy diff
  changed, or if the manifest already has a receipt. Under the account lock it
  applies the saved plan and writes an account receipt.
- **Staged policy changes take one manifest per phase.** A change that
  replaces policies or moves tiers is planned one changing phase at a time:
  each `release` applies that phase and says to run `release-plan` again
  ([staged policy changes](deterministic-policies.md#staged-policy-changes-rollout-step-6)).
- **Existing resources are imported by their own manifest.** When resources
  exist in Databricks but not in state, `release-plan` records the imports in
  the manifest and plans nothing else; `release` runs exactly those.
- **Rollback.** Merge `prod:rollback:<commit>` into `envs/prod/ack.txt` on
  `main` in a reviewed PR, then run `release-plan` with that older commit
  checked out.
- **`make maintain ENV=prod`** refuses unless HEAD is the last release
  receipt's commit, the checkout is clean, and the digests match the receipt.
- **No side door.** A promoted env (one with `promote_from`, or `prod`)
  changes only through `release` (a manifest's saved plans) and `maintain`; dev only through
  `rehearse`. Every other target that applies the data_access or workspace
  root refuses there and says which command to run. The gate is the env lock
  of the command that runs the target, checked by a nonce, the holder's pid and
  its start time, so a leftover or copied lock file never passes. This guards
  against accidents, not someone forging state on the deployment machine:
  protect that machine and its credentials.

  | Target | Promoted env (prod, stg, ...) | Rehearse env (dev) |
  |---|---|---|
  | `apply`, `apply-genie` | refused | only under `rehearse` |
  | `apply-governance` | only under `maintain` | only under `rehearse` or `ungovern` |
  | `rehearse` | refused (rehearse the source env) | the way to apply it |
  | `ungovern` | edits the governed-table list only; the next release applies it | edits, then applies governance |
  | `destroy`, `destroy-governance`, `destroy-genie` | needs `CONFIRM_DESTROY=<env>` on the command line | unchanged |
  | `release` | only a manifest from `release-plan` (the current one, or `MANIFEST=<path>`) | refused (rehearse it) |

  Not gated by the env side-door: `apply-account` (its own account manifest,
  account lock and approval),
  `enable-classification` (classification config only), `import`,
  `migrate-state`, `rebuild-governed-tables` (state or a local record), and
  `policy-abandon` (drops only a staged change that never switched, or whose
  switch was rolled back and still is, live). `policy-rebind` refuses a
  promoted env. Each
  moves a state serial or a committed file, so a manifest planned before it is
  refused.

## Drift between releases

`make audit-schema ENV=prod` is read-only and safe to schedule in CI: it exits
`1` on untagged sensitive columns or tags on deleted columns. Fix drift with
`make maintain ENV=prod` on the deployment machine. To run these on a
schedule, see [scheduled governance](scheduled-governance.md) (deterministic
envs use your own scheduler; the Databricks job is legacy-only).

Independent environments that should not inherit dev governance (for example
one per business unit): [Advanced](advanced.md#independent-environments).
