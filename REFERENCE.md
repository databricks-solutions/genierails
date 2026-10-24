# GenieRails reference: every command and setting

Every `make` target in [`shared/Makefile.shared`](shared/Makefile.shared) and every setting the Terraform roots read from `env.auto.tfvars` and `auth.auto.tfvars`. Run `make` from `aws/` or `azure/`. `make help` lists the targets with a `##` description.

For the champion flow step by step, see the [Dev-to-Prod Walkthrough](shared/examples/dev_to_prod/README.md) and its [reference](shared/examples/dev_to_prod/REFERENCE.md), which covers the same release commands in more depth plus the deterministic-governance setting rules. For where each stage runs, see [design section 17](shared/docs/design/deterministic-governance.md#17-where-each-command-runs) and [Pipelines](shared/docs/pipelines.md).

`shared/tests/test_reference_coverage.py` fails when this page misses a target or names one that doesn't exist.

## Stages

The **Stage** column uses the stages of [design section 17](shared/docs/design/deterministic-governance.md#17-where-each-command-runs):

| Stage | Where it runs | Changes Databricks? |
|---|---|---|
| **setup/local** | a person's machine or the deployment machine: one-time setup, authoring, local tools and GenieRails' own tests | authoring: no; setup: as noted |
| **PR check** | CI, every PR (`make check`) | no |
| **dev rehearse** | dev pipeline, or the deployment machine (`make rehearse ENV=dev`) | dev only |
| **promotion PR** | a person's machine; the output is committed in a PR (`make promote-to`) | no |
| **prod release-plan/release** | prod pipeline or the deployment machine, two phases with an approval between | prod |
| **maintain** | prod, on a schedule (`make maintain`) | prod, governance only |
| **operations** | deployment machine, manual, one approval per operation | prod or the account layer |

Applying targets refuse in CI (`CI=true/1/yes`, or the Azure Pipelines, Jenkins, GitLab, Buildkite or CircleCI markers). The one exception is `make rehearse ENV=dev` when dev has remote state with locking (`envs/dev/remote_state.hcl`). `GENIERAILS_ALLOW_CI_APPLY=1` exists only for GenieRails' own throwaway integration-test jobs.

## Parameters every target reads

| Parameter | Meaning |
|---|---|
| `ENV=<env>` | The env under `envs/`. Default `dev`. `ENV=account` selects the shared account layer, where a target allows it. Must be lowercase letters, digits, `_` or `-`, starting with a letter; make refuses anything else before a recipe runs. |
| `VERBOSE=1` | Print full PASS reports from validation (`validate`, `validate-generated`, `coverage-gate`, `promote`); also passed to classification and the coverage check. |
| `GENIERAILS_STATE_DIR` (environment variable) | Where the deployment machine keeps Terraform state, receipts, manifests and locks, outside the checkout. Unset: `<cloud>/.genierails-state` and the in-checkout layout. See [deployment machine](shared/docs/deployment-machine.md). |
| `ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,...` | `make account-plan` only: the tag-policy values the account manifest may remove (with `WAREHOUSE_ID=<id>` for the usage check). Sorted and recorded in the manifest; approving the manifest approves exactly these removals, and `make apply-account` refuses a different list. Unset: nothing is removed. See [tag-policy values](shared/docs/deterministic-policies.md#tag-policy-values). |

A deterministic env is one with `governance_mode = "deterministic"`; a legacy env is anything else.

## Commands

### Setup and environment

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make help` | none | List every target with a `##` description, and the reserved env names. | setup/local |
| `make setup` | `ENV`, `GITIGNORE=customer\|off` | Scaffold `envs/account` and `envs/<env>` (dirs, default `auth.auto.tfvars` and `env.auto.tfvars`, `data_access/` symlinks) and print the next steps for that env. In a git checkout whose `origin` isn't `databricks-solutions/genierails`, writes the `.gitignore` block that commits `envs/`; `GITIGNORE=customer` forces it, `GITIGNORE=off` skips it. | setup/local |
| `make init-env` | `ENV` | The same scaffolding as `setup`, without the next steps or the `.gitignore`. | setup/local |
| `make bootstrap-sp` | `ACCOUNT_ID` (required), `WORKSPACE_ID` (required; comma-separated for several), `ACCOUNT_PROFILE` (default `DEFAULT`), `WORKSPACE_PROFILE`, `SP_NAME` (default `genierails-deployer`), `MODEL_ENDPOINT` (default `databricks-claude-sonnet-4-6`), `TARGET_CATALOG`, `PLAN=1` (dry run), `YES=1`, `ROTATE_SECRET=1`, `BOOTSTRAP_AUTH_ENV=<env>`, `BOOTSTRAP_AUTH_OVERWRITE=1` | Create or reuse the deployment service principal and grant it what GenieRails needs, using an account-admin CLI profile (`scripts/bootstrap_sp.py`). By default a newly minted OAuth secret is printed once; with `BOOTSTRAP_AUTH_ENV`, a single-workspace run writes it to that env's `auth.auto.tfvars` as mode `0600` and prints only the secret id/expiry. It refuses symlinks and filled secrets unless `BOOTSTRAP_AUTH_OVERWRITE=1`. | setup/local |
| `make bootstrap-pipelines` | `ENV`, `CI=github\|azure-devops`, `ACCOUNT_ID` (required), `WORKSPACE_ID` (required; the env's workspace), `REPO=<owner>/<name>` (GitHub) or `ADO_ORG`, `ADO_ORG_ID`, `ADO_PROJECT` (Azure DevOps), `ACCOUNT_PROFILE`, `WORKSPACE_PROFILE`, `READONLY_SP_NAME` (default `genierails-<env>-readonly`), `CATALOGS`, `GENIE_SPACE_IDS`, `WAREHOUSE_ID`, `DEPLOYER_CLIENT_ID` (default: the committed `deployer_principal_id`), `GOVERNANCE_OWNERS`, `AGENT_OWNERS`, `PLAN=1`, `YES=1` | Create the env's read-only pipeline SP and the OIDC federation for each pipeline identity, grant the section 17 privileges, and print the repo variables, environments, branch protection and a `CODEOWNERS`. Writes nothing to the repo and makes no secret. `CI=true` (set by CI runners) is refused; pass `CI=` on the command line. See [Pipelines](shared/docs/pipelines.md#setting-up-the-pipelines). | setup/local |
| `make enable-classification` | `ENV` | Optional scripted way to turn on UC Data Classification for the env's footprint (a targeted data_access apply); discovers the tables of agents given only by ID first. You still review detections and auto-tagging in Catalog Explorer. Refuses in CI. | setup/local |
| `make migrate-state-dir` | `ENV`, `GENIERAILS_STATE_DIR` (environment variable) | Move the env's local Terraform state and runtime records from the checkout into `GENIERAILS_STATE_DIR`, holding the env lock; never overwrites. Refuses in CI. | setup/local |
| `make migrate-state` | `ENV` | One-time upgrade of pre-layer state into the account / data_access / workspace roots (state moves and imports). Refuses with `GENIERAILS_STATE_DIR` set or a `remote_state.hcl`; run it before `migrate-state-dir`. | setup/local |
| `make migrate-root-to-env` | `ENV` | Move working files left in the cloud root by an older layout (`auth.auto.tfvars`, `env.auto.tfvars`, `abac.auto.tfvars`, state, `ddl/`, `generated/`) into `envs/<env>`. Refuses if the target env already has working files. | setup/local |
| `make clean` | `ENV` | Delete the env's generated files (`generated/abac.auto.tfvars`, `masking_functions.sql`, `generated_response.md`, the derived ACL file), local Terraform state, `.terraform/` and apply fingerprints. Keeps `auth.auto.tfvars`, `env.auto.tfvars` and the split `abac.auto.tfvars` files. Refuses unless `ENV_DIR` is this checkout's real `envs/<env>`. | setup/local |
| `make clean-all` | none | Delete the whole `envs/` directory of this cloud, including every env's `auth.auto.tfvars`, `env.auto.tfvars` and local state. No confirmation. | setup/local |

### Authoring (agents and governance)

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make generate` | `ENV`, `SPACE="<agent>"`, `MODE=governance\|genie`, `COUNTRY=<codes>`, `INDUSTRY=<codes>`, `GENERATE_ARGS='<flags>'` | Run `generate_abac.py` in the env: import agents never captured before, find their tables and draft the governance config into `generated/`. Deterministic envs derive from `class.*` tags with no model call, record `generated/governed_tables.json`, propose `table_readers` entries and check `acl_groups` can read their tables; `SPACE=` is refused there. Refused in a deterministic promoted env (`promote_from` set, or `ENV=prod`): only `make promote-to` writes its `generated/` (release-plan refuses a regenerated one as a stale promotion); legacy promoted envs still generate. `SPACE=` (legacy) regenerates one agent and needs a prior full run. `MODE=governance` generates ABAC only; `MODE=genie` agent config only. `COUNTRY`/`INDUSTRY` override the env's `country`/`industry`. | setup/local |
| `make capture` | `SPACE="<agent>"` (required); `ENV` must be `dev` | Export one dev Genie agent's config to `envs/dev/agents/<agent>.space.json` (one read-only GET) and, in a deterministic env, re-record its tables in `generated/governed_tables.json`. | setup/local |
| `make split-agent-files` | `ENV`, `AGENT_FILE_NAMES=<old-key>=<file-name>,...` | Move the env's inline `genie_spaces` into `agents/<agent>.auto.tfvars` files, keeping Terraform addresses. Writes nothing unless Terraform gets exactly the same agents. Takes the env lock; refuses in CI. | setup/local |
| `make scaffold-treatments` | `ENV`, `TREATMENT=<existing>`, `ALLOW_UNKNOWN_TYPE=1` | Map unmapped native `class.*` tags. With `TREATMENT=` it reuses that mask (deterministic envs: a mask-library treatment, written to the env's `tag_treatments`); without it, legacy envs get fail-safe REVIEW stubs. | setup/local |
| `make materialize-treatment` | `ENV`, `TREATMENT=<treatment>` (required; a value in `shared/treatment_config.json`) | Add that treatment's mask policy and UDF for every governed catalog without any `tag_assignments`, so the env it is promoted to can use it. Refuses prod and promotion targets. Then rehearse and `promote-to`. | setup/local |
| `make suggest-treatment` | `CLASS=<class>`, `FORCE=1`; `ENV=dev` only | [AI helper] Draft a treatment suggestion for an unmapped class into `envs/dev/drafts/`. Calls the model in `ai_helper_endpoint`; refuses in CI. | setup/local |
| `make draft-genie-content` | `SPACE="<agent>"`, `FORCE=1`; `ENV=dev` only | [AI helper] Draft instructions, sample questions and benchmarks for a captured agent into `envs/dev/drafts/`. | setup/local |
| `make draft-custom-mask` | `TREATMENT=<name>`, `TYPE=<sql-type>`, `FORCE=1`; `ENV=dev` only | [AI helper] Draft a custom mask and its exact-output fixture into `envs/dev/drafts/custom_masks/`. | setup/local |
| `make test-custom-mask` | `TREATMENT=<name>`; `ENV=dev` only | Run the mask library's exact-output checks on a drafted custom mask (offline, then each case read-only on the dev SQL warehouse) and record the digests it passed. Calls no model; adopts nothing. | setup/local |
| `make adopt-custom-mask` | `TREATMENT=custom_<name>`, `FORCE=1` (replace an adopted type family); `ENV=dev` only | Copy a draft that `test-custom-mask` passed into `envs/dev/custom_masks/` and print the `tag_treatments` line to add. Calls no model; refuses in CI. | setup/local |
| `make ungovern` | `ENV`, `TABLE=cat.sch.tbl` (required), `YES=1` (skip the prompt) | Take a table no agent uses out of a deterministic env's governed set and remove its treatment tags and masks. In the rehearse env it applies governance under the env lock; in a promoted env it only edits `generated/`, for a PR and the next release. | setup/local |
| `make rebuild-governed-tables` | `ENV` | Rebuild a lost or unreadable `generated/governed_tables.json` from its git history, the deployed treatment tags and the generated config. Local only. | setup/local |

### PR check

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make check` | `ENV` | Read-only, safe in CI. In a scratch copy of the cloud dir: `validate`, generated files current, `validate-generated`, offline coverage check, mask-library unit tests, a structural `plan`; with `promote_from` set or `ENV=prod`, also `promote-to CHECK=1`. It validates tracked working-tree content and prints a non-failing note when that differs from the commit; `rehearse` and `release-plan` use the commit. Refuses an `ENV_DIR` override. | PR check |
| `make validate` | `ENV` (a workspace env or `account`), `COUNTRY`, `INDUSTRY`, `VERBOSE=1` | Validate the split config: the account layer for `ENV=account`, otherwise the env's data_access and workspace `abac.auto.tfvars` (and `masking_functions.sql`). | PR check |
| `make validate-generated` | `ENV`, `COUNTRY`, `INDUSTRY`, `VERBOSE=1` | Validate the env's effective `generated/` files (the runtime overlay when current, else the committed ones), including the one-mask-per-column guard. | PR check |
| `make coverage-gate` | `ENV`, `VERBOSE=1` | Fail on any sensitive column in the generated config that no mask covers. Every plan and apply also runs the coverage check against live tags. | PR check |
| `make plan` | `ENV` (a workspace env or `account`), `APPLY_FLAGS` | Terraform plan: the account layer for `ENV=account`, otherwise data_access then workspace. Re-reads live tags and DDL first (unless a caller already did), so it may re-split `generated/`. A deterministic data_access plan shows the end state of a staged policy change. | PR check |

### Promotion

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make promote-to` | `ENV=<destination>`, `FROM=<source>`, `CATALOG_MAP="src=dest,..."`, `CHECK=1`, `ALLOW_DISABLE_AUTO_TAGGING=1`, `REUSE_SOURCE_AUTH=1` with `WORKSPACE_HOST`/`WORKSPACE_ID` | Promote the rules (not tag assignments) into `ENV` using `promote_from` and `catalog_map` from its `env.auto.tfvars`. `FROM=`/`CATALOG_MAP=` on the command line override and update those settings. Carries agent files and adopted custom masks, writes `generated/expected_classification.json`, and prints the changed files by owner. `REUSE_SOURCE_AUTH=1` (command line only, for one SP in both envs) fills a destination `auth.auto.tfvars` still in its template state with the source's account ID, client ID and secret; the workspace host and ID are the destination's (from the file, else `WORKSPACE_HOST`/`WORKSPACE_ID`; never the source's), a set `deployer_principal_id` must equal the source client ID, and a configured file is never overwritten (a shell variable is ignored, and a symlinked or special source or destination file is refused). An exported `MAKEFLAGS` carrying `REUSE_SOURCE_AUTH=1` counts as the command-line opt-in, since make cannot tell them apart: the flag guards against accidental reuse and is not a security boundary. A `catalog_map` entry that maps a catalog to itself is refused unless the destination sets `allow_identity_catalog_map = true` (separate metastores only). A refused promotion restores everything it touched. `CHECK=1` writes nothing: it re-runs in a scratch copy and fails if the committed output differs. | promotion PR (`CHECK=1`: PR check) |
| `make promote` | same env: `ENV`; cross env: `SOURCE_ENV`, `DEST_ENV`, `DEST_CATALOG_MAP="src=dest,..."` (required), `ALLOW_DISABLE_AUTO_TAGGING=1` | With `DEST_ENV`: the cross-env promotion `promote-to` wraps, with explicit arguments every time (nothing saved). Without it: split the env's `generated/` into the account, data_access and workspace `abac.auto.tfvars` files and validate them. Cross-env promotion refuses a destination `enable_auto_tagging = false` unless `ALLOW_DISABLE_AUTO_TAGGING=1`. | promotion PR |

### Dev rehearse

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make rehearse` | `ENV` (not `prod`, not a promoted env), `VERIFY_KEY_COLUMN`, `VERIFY_SPEC`, `WAREHOUSE_ID` | Live derive, `validate-generated`, `coverage-gate`, apply all layers, then `verify-access` as `_verify-access-gate` (pass prints `ALL EFFECTIVE`); stops at the first failure. Does **not** run `make check`. Deterministic envs check the deployer, readers, groups and adopted custom masks first, then take the env lock and write a rehearse receipt for the commit. When unchanged inputs reuse an earlier pass for a new commit, rehearse does **not** re-check live dev for out-of-band drift. If dev was changed by hand, revert the live change and run a real apply/rehearse, or abandon/re-stage the unfinished policy change. Saves the proven row-pairing keys (deterministic envs print them to commit). | dev rehearse |
| `make verify-access` | `ENV`, `VERIFY_KEY_COLUMN`, `VERIFY_SPEC=<file>`, `WAREHOUSE_ID`, `KEEP_PRINCIPALS=1`, `VERIFY_REQUIRE_MASKS=1` | Prove masks and row filters take effect on the live workspace by querying as per-tier test principals (pass: `ALL EFFECTIVE`). Picks and proves a row-pairing key per masked table unless `verify_key_columns` or `VERIFY_KEY_COLUMN` names one. Standalone re-check; `rehearse` and `release` already run it as `_verify-access-gate`. See [effective-access verification](shared/docs/effective-access-verification.md). | dev rehearse |
| `make verify-access-spec` | `ENV`, `VERIFY_KEY_COLUMN`, `VERIFY_SPEC` | Print the derived effective-access checks. Needs no workspace or cluster. | setup/local |

### Prod release

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make release-plan` | `ENV`, `VERIFY_KEY_COLUMN`, `VERIFY_SPEC`, `ACK_UNCLASSIFIED`, `WAREHOUSE_ID`, `RELEASE_REMOTE` (default `origin`), `RELEASE_BRANCH` (default `main`) | Deterministic envs only. Check provenance (a passing clean rehearsal of HEAD in the env's source env, its `promote_from` (default `dev`), a clean checkout, HEAD on freshly fetched `<RELEASE_REMOTE>/<RELEASE_BRANCH>`, matching digests, not older than the last release); then under the env lock re-derive from live tags with deployed policy names kept, validate, coverage, prove row-pairing keys, audit the rulebook, run the reader and refuse-weakening checks, save a plan for every root and write one immutable manifest. Changes nothing in Databricks. | prod release-plan/release |
| `make release` | `ENV`; deterministic envs: optional `MANIFEST=<path>` (pipelines name the approved manifest explicitly); also `VERIFY_KEY_COLUMN`, `VERIFY_SPEC`, `WAREHOUSE_ID`; legacy envs: `ACK_UNCLASSIFIED` | Deterministic envs: `make release ENV=<env>` releases the env's current manifest (the one the last `release-plan` wrote), printing its path, commit and changes first; it applies only that manifest's saved plans (data_access through the staged policy controller), then `verify-access`, then writes the release receipt `maintain` checks. The saved plans contain credentials (the deployer's client secret): written owner-only (umask 077), they are `0400` under the git-ignored `.genierails-state`, and are deleted once the release receipt (or, for a manifest that applies one phase or only imports, its phase record) is written. A failed release keeps its plans for retry; a newer `release-plan` deletes the saved plans of older unreleased manifests while retaining their manifests and audit records. It refuses, asking for a fresh `release-plan`, if there is no current manifest, the manifest was already released or superseded by a newer `release-plan`, or the commit, a state serial, a saved plan or live UC changed. Legacy envs: derive, validate, coverage, promote, prove keys, audit the rulebook, apply all layers and verify, in one run. | prod release-plan/release |
| `make verify-access-keys` | `ENV`, `VERIFY_KEY_COLUMN`, `VERIFY_SPEC`, `WAREHOUSE_ID` | As the admin only (no test principals, no grants), pick and prove a row-pairing key for every masked table. `release-plan` and legacy `release` run it before applying. | prod release-plan/release |
| `make evidence` | `ENV`, `GENIERAILS_EVIDENCE_INTEGRATION=1` (environment variable; live state), `WAREHOUSE_ID` | Write JSON and Markdown compliance evidence (`evidence_report.py`). See [evidence report](shared/docs/evidence-report.md). | prod release-plan/release |
| `make certify` | none | Removed. Always fails and points to `make release` (gate it behind your approval, with `make plan` to review). | none (removed) |

### Maintain

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make maintain` | `ENV`, `VERIFY_SPEC`, `VERIFY_KEY_COLUMN` | Reconcile governance: `audit-schema`, `derive-assignments`, `coverage-gate`, `validate-generated`, `audit-rulebook`, then `apply-governance`, under the env lock. Never widens access past the coverage check and never changes Genie. A deterministic env runs it only from the last released commit with matching digests. | maintain |
| `make derive-assignments` | `ENV` | Re-derive only `tag_assignments` from live native `class.*` tags with the promoted rules unchanged (no model call); refreshes `ddl/_fetched.sql`. Deterministic envs write the result to the ignored runtime overlay `generated/.runtime`. `release-plan`, `release` and `maintain` run it. | maintain |
| `make audit-schema` | `ENV` | Report untagged sensitive-looking columns and stale tag assignments in managed tables. The first step of `maintain`. | maintain |
| `make audit-rulebook` | `ENV` | Report applied tags (`class.*` and governed keys) that no policy or mask covers. Exit 1 is drift, 2 an error. | maintain |
| `make apply-governance` | `ENV`, `APPLY_FLAGS`, `VERIFY_SPEC`, `VERIFY_KEY_COLUMN` | Apply the account and data_access layers only (no Genie agents). In a deterministic env it runs only under the lock of `rehearse`, `ungovern` (rehearse env) or `maintain` (promoted env). | maintain |
| `make generate-delta` | `ENV` | [Legacy] Model or name-regex incremental `tag_assignments`. Use native `class.*` tags and `make maintain` instead. | maintain |

### Operations

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make account-plan` | `RELEASE_REMOTE` (default `origin`), `RELEASE_BRANCH` (default `main`), `ALLOW_TAG_VALUE_REMOVAL=<key>:<value>,...` with `WAREHOUSE_ID=<id>` | Plan the shared account layer while holding the account env lock for the whole plan: refuses unless the checkout is clean and HEAD is exactly the freshly fetched `<RELEASE_REMOTE>/<RELEASE_BRANCH>` tip, then writes one immutable account manifest (plan hash for a changing plan, account input hashes, the tag-policy diff: values to add, values to remove, live values left in place) to approve before `make apply-account`. A no-change result (no Terraform changes and no tag values to add or remove) is recorded without retaining its credential-bearing saved plan and is not selectable by `apply-account`. Terraform ignores tag-policy values, so a plan that only adds or removes tag values shows no Terraform changes; it is not a no-change result, keeps its saved plan and is applied like any other. A changing saved plan contains credentials: it is `0400` under the git-ignored `.genierails-state` and is deleted after a successful apply. A failed apply keeps its saved plan for retry; a newer `account-plan`, a no-change one included, deletes saved plans belonging to older unapplied manifests while retaining their manifests and audit records, and `apply-account` refuses those older manifests as superseded. No-change manifests contain no plan or secret and may be removed as housekeeping. A concurrent rehearsal or release that reaches account tag sync fails fast on this lock before applying anything; retry it after account planning finishes. It prints the manifest summary, tag-value removals first. Nothing is removed unless `ALLOW_TAG_VALUE_REMOVAL` names it; each named value must be live, no longer desired, not a deterministic generation, and unused: no catalog, schema, table, column or volume assignment in `system.information_schema` (read through `WAREHOUSE_ID`) and no ABAC policy reference on any catalog, schema or table the deployer can list (every one, unbound and browse-only catalogs included, not only tagged ones). A value in use, or whose use can't be checked (a failed or truncated query, a listing or policy read that fails anywhere), is refused and no manifest is written. A plan that deletes or replaces a tag policy is refused too. Changes nothing live. | operations |
| `make apply-account` | optional `MANIFEST=<path>` (default: the one unreceipted account manifest for HEAD) | Apply the shared account layer (groups, tag policies) from the approved account manifest's saved plan, under the account lock, then write an account receipt. Prints the manifest summary, then adds the missing tag values and removes exactly the values the manifest approved (after re-checking, under the lock, that they're still unused); other live values are left in place. Refuses if the planned additions or removals no longer match live, or if `ALLOW_TAG_VALUE_REMOVAL` is set to anything other than what the manifest records (removals are chosen at `account-plan`). While any env is deterministic, `make apply ENV=account` routes here, and `rehearse` and `release` only add allowed treatment-tag values (they refuse a non-empty account plan). Refuses in CI. | operations |
| `make sync-tags` | `ENV` | Sync tag-policy values in the account layer. Add-only in every governance mode, legacy-only checkouts included: it adds missing values and never removes one; live values this checkout doesn't desire (on a shared metastore, other repos' or teams' values) are listed as left in place and the sync succeeds. `TAG_SYNC_MODE=replace` (and any mode but `add-only`) is refused; removal goes only through `account-plan ALLOW_TAG_VALUE_REMOVAL=...` and `apply-account`. The script's `--dry-run` reports exactly what a run would do, refusals included. | operations |
| `make wait-tag-policies` | `REQUIRED_VALUES_TFVARS=<abac.auto.tfvars>` | Wait until the account tag policies (and the values that file needs) are visible to the FGAC APIs. `apply` and `release` run it. | operations |
| `make detach-agent` | `ENV`, `SPACE="<agent>"` (required) | Step 2 of removing an agent: forget its workspace-state addresses, never deleting the agent. Refuses unless HEAD is the last released commit and that release had the agent with `detached = true`. Takes the env lock. | operations |
| `make policy-status` | `ENV` | Read-only: show a deterministic env's staged policy journal (live generation, the one waiting to retire, any unfinished change). | operations |
| `make policy-abandon` | `ENV` | Drop an unfinished staged policy change that never switched a column or moved a principal, so a corrected config can start a new one. Takes the env lock, re-reads live tags/DDL under it and re-splits the result (as `rehearse` does; a failed refresh, or a split whose non-treatment tag assignments still differ from the generated config's after that re-split refuses before anything is planned), and re-runs the coverage check on the abandon render (the previous generation it leaves live) before planning; the check's result records that generation as `kept_from`. Works after the generated config drifted from the change (live `class.*` tags or treatment settings changed): it still deletes only the change's own policies and keeps the previous generation's recorded values, once that generation's snapshot still hashes to its fingerprint. Its saved plan contains credentials: it is written `0600` (umask 077) in the env's policy ledger (`data_access/.policy_generations/abandon`, git-ignored), deleted once the journal records the abandon or when the plan check refuses it (it can never be applied), and kept when the abandon apply fails. A run refused because the config differs from an unfinished change says to run this, then re-run. | operations |
| `make policy-rebind` | `ENV` | Dev only (refuses a promoted env and CI, even with `GENIERAILS_ALLOW_CI_APPLY=1`). After principals of a rolled-back staged change were deleted and recreated, re-point its policies at the recreated principals (checked live), then create exactly the grants the deleted principals lost (verify-access), each from a saved plan under the env lock, after re-reading and re-splitting live tags/DDL under it for the coverage check (a failed refresh, a split that still differs from the generated config after the re-split, or treatment values that differ from the change's refuses before anything is planned); then run `make policy-abandon`. Its saved plans contain credentials: they are written `0600` (umask 077) in the policy ledger's `rebind` directory, a plan the plan check refuses is deleted, and the others are kept while the rebind is unfinished (a re-run checks the held-back grants against the protect plan's copy), and deleted once the journal records the finished rebind (or the abandon). See [deterministic-policies.md](shared/docs/deterministic-policies.md). | operations |
| `make migrate-plan` | `ENV` | [migration] Read-only dry run of moving a legacy env to deterministic: per column and principal, the `SELECT` and `CAN_RUN` the cutover revokes, frozen row filters, tag state moves, the `ack.txt` entries it needs and a proposed `table_readers` (written to the state dir). In a deterministic env it also lists masks GenieRails didn't create, which block the cutover. See [legacy migration](shared/docs/legacy-migration.md). | operations |
| `make genie-adopt-preflight` | `ENV` | Read-only: check every Genie agent created by an earlier version can be adopted (ID file, host, GET 200) before migrating. | operations |
| `make import` | `ENV` (a workspace env or `account`) | Import existing resources into the layer state (`scripts/import_existing.sh`): the account layer (groups and tag-policy keys, only from `envs/account`), or the env's data_access and workspace layers. It changes Terraform state, not tag-policy values; with a deterministic env, the account state change invalidates any pending account manifest, so run `make account-plan` again afterwards. Run by hand, the script derives `ENVS_DIR` from the env dir; see [import existing](shared/IMPORT_EXISTING.md). | operations |
| `make destroy` | `ENV` (a workspace env or `account`), `CONFIRM_DESTROY=<env>`, `DESTROY_FLAGS` | Destroy the account layer, or the env's workspace then data_access layers. A promoted deterministic env needs `CONFIRM_DESTROY=<env>` on the command line. | operations |
| `make destroy-governance` | `ENV`, `CONFIRM_DESTROY=<env>`, `DESTROY_FLAGS` | Destroy only the env's data_access layer (leaves workspace and Genie). | operations |
| `make destroy-genie` | `ENV`, `CONFIRM_DESTROY=<env>`, `DESTROY_FLAGS` | Destroy only the env's workspace layer (leaves data_access and ABAC). | operations |

### Direct applies (legacy envs and the deployment machine)

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make apply` | `ENV` (a workspace env or `account`), `APPLY_FLAGS`, `VERIFY_SPEC`, `VERIFY_KEY_COLUMN`, `FORCE_APPLY=1` | Apply the account layer, or account then data_access then workspace for a workspace env (re-reading live tags and re-splitting `generated/` first). Business access is granted only once the coverage check passes. In a deterministic env it runs only under `rehearse`'s env lock, and `APPLY_FLAGS` is refused for its data_access layer. | setup/local |
| `make apply-genie` | `ENV`, `APPLY_FLAGS` | Apply only the workspace layer (groups and Genie agents), assuming governance is applied. New `CAN_RUN` stays blocked until the coverage check passes. In a deterministic env it runs only under `rehearse`'s env lock. | setup/local |

### GenieRails' own tests

| Command | Parameters | Purpose | Stage |
|---|---|---|---|
| `make test-unit` | none | Run `pytest shared/tests/` (no Databricks, model or Terraform credentials). | setup/local |
| `make test-ci` | `ACCOUNT_ADMIN_ENV=<file>` (default `shared/scripts/account-admin.<cloud>.env`), `WAREHOUSE_ID`, `SCENARIO=<name>`, `FAIL_FAST=0`, `KEEP_DATA=1` | Unit tests, provision a fresh workspace, run the integration scenarios, tear down (always). | setup/local |
| `make test-ci-parallel` | `ACCOUNT_ADMIN_ENV`, `MAX_PARALLEL`, `SCENARIOS`, `KEEP_ENVS=1`, `NO_FAIL_FAST=1` | Provision one workspace per scenario and run them concurrently, then tear down. | setup/local |
| `make integration-test` | `ENV` (dev side), `ITEST_DEST_ENV` (default `prod`), `WAREHOUSE_ID`, `KEEP_DATA=1` | End-to-end legacy run: test catalogs, setup, apply, generate, promote, apply prod, verify, teardown. | setup/local |
| `make test-all` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Every scenario below, one after another (continues on failure). | setup/local |
| `make test-champion` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: dev to prod champion flow (generate, rehearse, promote, release, maintain). | setup/local |
| `make test-quickstart` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: one agent, one catalog. | setup/local |
| `make test-multi-catalog` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: one agent spanning two catalogs. | setup/local |
| `make test-multi-space` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: two agents with separate catalogs. | setup/local |
| `make test-per-space` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: incremental per-agent generation. | setup/local |
| `make test-promote` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: multi-agent dev to prod promotion. | setup/local |
| `make test-multi-env` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: two independent envs. | setup/local |
| `make test-attach-promote` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: attach to a UI-created agent, then promote. | setup/local |
| `make test-self-service-genie` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: central governance applies ABAC, BU teams apply their own agents. | setup/local |
| `make test-abac-only` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: governance only, then add an agent. | setup/local |
| `make test-multi-space-import` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: import two UI-configured agents in one `generate`. On `main` its recipe is missing its `--auth-file` line, so it also absorbs the `test-schema-drift` rule and then runs the schema-drift scenario. | setup/local |
| `make test-schema-drift` | none on `main` | Meant as the scenario for tag drift after `ALTER TABLE` adds, drops or renames a column. On `main` it runs nothing ("Nothing to be done"), because its rule line sits inside `test-multi-space-import`'s recipe. | setup/local |
| `make test-genie-only` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: `genie_only = true` with a minimal-privilege SP. | setup/local |
| `make test-country-overlay` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: country overlays (generation only). | setup/local |
| `make test-industry-overlay` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: industry overlays and a country plus industry combination. | setup/local |
| `make test-genie-import-no-abac` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: import an agent and deploy to prod without ABAC. | setup/local |
| `make test-aus-bank-demo` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: Australian bank demo walkthrough. | setup/local |
| `make test-india-bank-demo` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: India bank demo walkthrough. | setup/local |
| `make test-asean-bank-demo` | `ENV`, `WAREHOUSE_ID`, `KEEP_DATA=1` | Scenario: ASEAN bank demo walkthrough. | setup/local |

## Settings

Make passes each layer the env's `auth.auto.tfvars`, `env.auto.tfvars` and `abac.auto.tfvars` (plus tool-written files) as Terraform variable files. `envs/<env>/data_access/` links to the env's own `auth.auto.tfvars` and `env.auto.tfvars`; `envs/account/auth.auto.tfvars` links to the selected workspace env's. Types and defaults below are the roots' `variable` blocks in [`shared/roots/`](shared/roots/). Use placeholders in examples; never commit real credentials.

### `auth.auto.tfvars`

Credentials only. Never commit it.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `databricks_account_id` | string | required by the account and data_access roots; `""` in workspace | The Databricks account ID. May be empty with `genie_only = true`. |
| `databricks_client_id` | string | required | Application (client) ID of the service principal that applies this env. A deterministic env applies only when it equals `deployer_principal_id`. |
| `databricks_client_secret` | string (sensitive) | required | That service principal's OAuth secret. |
| `databricks_workspace_id` | string | required by workspace; `""` elsewhere | The workspace ID. |
| `databricks_workspace_host` | string | required by workspace and data_access; `""` in account | The workspace URL. |
| `databricks_account_host` | string | `"https://accounts.cloud.databricks.com"` | Account console API URL. Azure must set `https://accounts.azuredatabricks.net`. |

### `env.auto.tfvars`: agents, warehouse and scope

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `genie_spaces` | list of objects: `key`, `name`, `genie_space_id`, `sql_warehouse_id`, `uc_tables`, `acl_groups`, `delete` (bool), `captured` (bool, set by the agent-file loader), plus the optional `declared_footprint` the generator reads | `[]` | Agent definitions. **Preferred:** one file per agent in `agents/<agent>.auto.tfvars` (keys `key`, `name`, `genie_space_id`, `acl_groups`, `detached`, `delete`, `uc_tables`, `sql_warehouse_id`; see [agent.auto.tfvars.example](shared/examples/dev_to_prod/agent.auto.tfvars.example)), merged in after `env.auto.tfvars` with this list left `[]`. **Also accepted:** the same objects inline here. An empty `genie_space_id` means GenieRails creates and manages the agent; an ID attaches to an existing one. `acl_groups` is who gets `CAN_RUN` (`[]` is nobody; required in deterministic envs). `delete = true` deletes instead of detaching when the agent is removed. |
| `genie_space_id` | string | `""` | Legacy single-agent path: the one agent's ID. |
| `genie_space_title` | string | `""` (workspace); `"Genie Space"` (account, data_access) | Legacy single-agent path: a title alone also creates an agent. |
| `sql_warehouse_id` | string | `""` | Shared warehouse for every agent without its own; `""` auto-creates a serverless one. |
| `warehouse_name` | string | `"ABAC Serverless Warehouse"` | Name of the auto-created warehouse. |
| `uc_tables` | list(string) | `[]` | Tables to govern beyond the agents' (`catalog.schema.table` or `catalog.schema.*`). The governance-only path sets only this. |
| `uc_catalog` | string | `""` | Legacy prefix: makes a schema-relative `uc_tables` or `table_readers` entry fully qualified. |
| `genie_only` | bool | `false` | Workspace root: skip account-level operations so the SP needs only Workspace Admin; `groups` must be empty and agent ACLs are skipped (self-service Genie). |
| `serverless_usage_policy_id` | string | `""` | AWS provider issue #5985 workaround for scripted classification on a fresh workspace. Normally leave empty. If `make enable-classification` reports `Usage policy ID must not be empty`, a Workspace Admin (or *Serverless usage policy: Manager*) must create a serverless budget/usage policy bound to this workspace; Account Admin alone may get `missing CreateBudgetPolicyPermission`. Set this to that policy UUID in `auth.auto.tfvars`. |

### `env.auto.tfvars`: governance and access

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `governance_mode` | string, `"legacy"` or `"deterministic"` | `"legacy"` | Selects deterministic governance (the template and new workspace envs set `"deterministic"`). |
| `access_tier_groups` | list(string), unique, non-empty | `[]` | IdP groups ordered most to least privileged: the first sees raw, the last fully masked, any between partial. |
| `table_readers` | map(list(string)) | `null` | Deterministic envs: `"cat.sch.tbl"` or `"cat.sch.*"` to the tier groups or `raw_exempt_principals` granted `SELECT`; the only source of `SELECT`. See [access](shared/docs/access.md). |
| `raw_exempt_principals` | list(string), group names (no `@`) | `[]` | Groups that see raw values except for never-raw treatments. |
| `treatment_versions` | map(object({ partial = string })) | `{}` | The reviewed partial mask version per treatment. |
| `tier_access_overrides` | map(map(string)), values `raw`, `partial` or `full` | `{}` | Per-treatment access for an intermediate tier group. |
| `column_overrides` | any | `{}` | Per-column `{ partial = "<version>" }`, `{ treatment = "<stricter>" }` or `{ keep_current = true }`. |
| `row_filters` | list(object({ table, column, values_by_group = map(list(string)) })) | `[]` | Declared row filters; rules on one table are ANDed. |
| `require_acl_groups` | bool | `false` | Legacy envs: refuse an agent without `acl_groups`. |
| `manage_groups` | bool | `false` | Only in `envs/account/env.auto.tfvars`, for a demo or greenfield account: create groups instead of looking up IdP-synced ones. A workspace env may not set it to `true`. |
| `business_access_enabled` | bool | `null` | Deprecated and ignored (make warns while it is set). `false` does not revoke access. |

The settings the rules above depend on are explained in more depth, with the rules make enforces for each, in the [walkthrough reference](shared/examples/dev_to_prod/REFERENCE.md#deterministic-governance-settings).

### `env.auto.tfvars`: classification, coverage and verification

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `enable_classification` | bool | `false` | Turn on UC Data Classification for the footprint's schemas; generation becomes fail-closed on missing `class.*` results. |
| `enable_auto_tagging` | bool | `null` (data_access; the workspace root declares `false`) | `null` keeps the catalog's UI setting; `true` sets GenieRails' supported `class.*` auto-tag list; `false` disables auto-tagging. |
| `coverage_acknowledged_columns` | list(string) | `[]` | `catalog.schema.table.column` names reviewed as not sensitive, so a table's first grant doesn't block on them. |
| `coverage_gate_max_age` | string (Terraform duration) | `"6h"` | Oldest live refresh of tags and DDL a passing coverage check may rest on (at most 24h). |
| `verify_key_column` | string | `""` | One row-pairing key for `verify-access` on every table that has it. |
| `verify_key_columns` | map(string) | `{}` | Per-table row-pairing key (`"catalog.schema.table" = "column"`); saved after a passing `verify-access`. |

### `env.auto.tfvars`: promotion and helpers

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `promote_from` | string | `""` | In the destination env: the env `promote-to` promotes from. No resource reads it. |
| `catalog_map` | any (map, or the legacy `"a=b,c=d"` string) | `{}` | In the destination env: source-to-destination catalog renames for `promote-to`. A catalog mapped to itself (`"paycat" = "paycat"`) is refused before anything is written, by `promote-to` and by every command that uses the env (`plan`, `apply`, `import`, `maintain`, `release-plan`, `release`, and `rehearse` of its source), even when the file was edited by hand: on a shared metastore the destination's state would adopt the source's masks, and a later retire in the source would drop them. No resource reads it. |
| `allow_identity_catalog_map` | bool | `false` | In the destination env, only when it and the source use **separate metastores**: `true` lets `catalog_map` map a catalog to itself. Only the boolean `true` in the destination's own file counts (no command-line flag; never copied from the source); `promote-to` keeps it. No resource reads it. |
| `ai_helper_endpoint` | string | `""` | In `envs/dev`: the model serving endpoint the AI helpers call. Governance never reads it. |

### `env.auto.tfvars`: scheduled governance (data_access root; legacy envs only, deprecated)

Off by default, and legacy envs only: make refuses `enable_scheduled_governance = true` in a deterministic env, which schedules `make maintain` / `make audit-rulebook` / `make audit-schema` itself. See [scheduled governance](shared/docs/scheduled-governance.md) and [`scheduled_governance.auto.tfvars.example`](shared/scheduled_governance.auto.tfvars.example).

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `enable_scheduled_governance` | bool | `false` | Legacy envs only (deprecated): create a scheduled job that runs the read-only governance checks. Refused in a deterministic env. |
| `scheduled_governance_mode` | string | `"check"` | `check` (read-only audits) or `legacy` (audit, generate-delta, then `validate_abac.py --coverage-gate` against the generated or split config; never apply). |
| `scheduled_governance_env` | string | `"prod"` | The env the job checks. |
| `scheduled_governance_cloud` | string | `"aws"` | `aws` or `azure`: the cloud dir holding that env. |
| `scheduled_governance_catalog` | string | `""` | Legacy mode only: the catalog for generate-delta. |
| `scheduled_governance_config_source` | string | `""` | Runtime-visible envs root with `account/` and the env (for example a Volume path); required when enabled, because the job's Git checkout has only committed files. Never put `auth.auto.tfvars` or another secret there. |
| `scheduled_governance_cron` | string (Quartz cron) | `"0 0 6 * * ?"` | Schedule. |
| `scheduled_governance_timezone` | string | `"UTC"` | Schedule timezone. |
| `scheduled_governance_git_url` | string | `""` | Repo the job checks out for code; required when enabled. |
| `scheduled_governance_git_branch` | string | `"main"` | Branch to check out. |
| `scheduled_governance_git_provider` | string | `"gitHub"` | Git provider name the Jobs API expects. |
| `scheduled_governance_serverless_client` | string | `"2"` | Serverless environment client version. |
| `scheduled_governance_dependencies` | list(string) | `["python-hcl2>=7.3.1,<8", "databricks-sdk", "pyyaml", "requests"]` | PyPI packages for the job. |
| `scheduled_governance_notification_emails` | list(string) | `[]` | Who is emailed when the job fails. |
| `scheduled_governance_job_name` | string | `""` (a generated name) | Override the job name. |

Before enabling legacy scheduled coverage, legacy envs generated before
`gr_treatment` existed must run `make generate ENV=<env>`, then
`make validate-generated ENV=<env>` and `make plan ENV=<env>`. Expect existing
legacy mask policies to be replaced by `gr_mask_*` ones; review the plan, then
run `make apply ENV=<env>` to add `gr_treatment` tags. Otherwise every scheduled
run fails with `classified but unprotected ... no gr_treatment`.

### `env.auto.tfvars`: settings make reads but Terraform doesn't

These have no `variable` block (Terraform only warns that they're undeclared); the scripts behind `make` read them.

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `deployer_principal_id` | string | none (required in deterministic envs) | Application ID of the SP that applies this env, committed (not secret). Policies exempt it; `rehearse`, `release-plan`, `release` and `maintain` refuse unless `databricks_client_id` matches it. Each env sets its own. See [access](shared/docs/access.md#the-deployer-sp-deployer_principal_id). |
| `tag_treatments` | map(string), `"class.<name>" = "<treatment>"` | none | The env's mapping from a native class to a mask-library or adopted custom treatment; `scaffold-treatments` writes it and promotion carries it. |
| `group_map` | map(string), `dev_group = "prod_group"` | none | Deterministic destinations: renames groups during promotion; a change needs a `<env>:weaken:group_map:<dev>=<target>` acknowledgement. See [access](shared/docs/access.md#group-names-in-prod-group_map). |
| `country` | string, comma-separated codes (`ANZ`, `IN`, `SEA`) | `""` | Country identifier overlays for `generate`; `COUNTRY=` overrides it. |
| `industry` | string, comma-separated codes | `""` | Industry overlays for `generate`; `INDUSTRY=` overrides it. |
| `declared_footprint` | list of table FQNs or `{ table, columns }` | `[]` | Also allowed per agent: greenfield discovery input that bounds classification, coverage and table grants. |

### Set by tools, not by hand

`make` and the generator write these; editing them by hand isn't supported.

- Set by the runner (`scripts/terraform_layer.sh`), which refuses a caller value: `env_dir`, `workspace_state_file`, `data_access_state_file`, and `expected_pre_release_withholding` (only for `make check`'s structural workspace `plan` against empty state, proven by a one-use token from `make check`, so that plan doesn't fail the Genie `CAN_RUN` withheld check; every other plan and apply keeps it). Also set by make and its scripts: `genie_id_dir`, `masking_sql_file`. The runner also exports `GENIERAILS_MASKING_BASE_DIR` (the env dir) for the data_access layer: `masking_functions_drop`'s destroy-time drop resolves its recorded relative SQL and auth paths against it, so a destroy from any checkout finds them. `masking_functions` records no paths, so a plan from another checkout doesn't replace it; the first plan after upgrading from a version that recorded them shows one drop-free re-key (see [deterministic-policies.md](shared/docs/deterministic-policies.md#masking-functions-and-checkout-paths)). Set only by the staged policy controller, which the runner checks with a token from the env lock, refusing any caller `-var`, `TF_VAR_`, var file or `TF_CLI_ARGS` value: `policy_stage` (staged policy phases; make's own `preview` for `plan` and `destroy` for `destroy` are the exceptions), `held_access_grants` and `rebind_table_grants` (`make policy-rebind`).
- Written to tool-owned tfvars files: `discovered_uc_tables`, `discovered_table_agents`, `genie_space_id_to_name` (`discovered_uc_tables.auto.tfvars`), `genie_space_acl_groups` (derived ACLs), `classification_existing_schemas`, `classification_all_schemas`, `classification_existing_auto_tag_configs` (`classification.auto.tfvars`).
- Generated into `abac.auto.tfvars` and split per layer by `promote`: `groups`, `group_members`, `tag_policies`, `tag_assignments`, `fgac_policies`, `genie_space_configs`, `genie_acl_groups`, `genie_space_description`, `genie_sample_questions`, `genie_instructions`, `genie_benchmarks`, `genie_sql_filters`, `genie_sql_expressions`, `genie_sql_measures`, `genie_join_specs`.
