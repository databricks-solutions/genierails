# Deterministic-governance E2E driver

The driver encodes the champion flow from
`shared/docs/design/deterministic-governance.md` sections 9, 12, 13 and 17,
plan scenarios S1–S16, cross-cutting
manifest/concurrency checks, the report-only migration operation, and the
migration cutover. A scenario that is expected to fail until a fix lands
carries `known_failure="<branch>"`: it reports `ERROR (expected until ...)`, and
if it passes its status is `UNEXPECTED PASS — remove known_failure ...` and the
run fails, so the marker gets removed. No scenario carries one now: the
cutover's marker (fix/migration-adopt-order, merged as #146) was removed with
the staged coverage-check fix (fix/staged-migration-coverage-loop), so a
cutover pass is a PASS. The cutover's protection check accepts only the column retags
migrate-plan printed, and only once the replacement is live: for each adopted
column, the `gr_mask_<value>_full` (and `_partial`, unless never-raw) policy and
function exist on that table, and every principal the legacy policy masked is
still masked by one of them. Only then may the legacy policy that used that
column's "mask today" function on that table, and that function, disappear. A
legacy `redact`/`compensation_redacted` column may only become a never-raw
treatment. Lifecycle mutations
are issued only as public `make` targets. Read-only snapshots cover UC grants,
column tags, effective ABAC policies, mask functions, Genie exports and ACLs,
Terraform serials, receipts, and manifests.

Always inspect the plan first; this neither loads auth nor invokes `make`:

```bash
python3 shared/scripts/e2e/driver.py --cloud aws --dry-run \
  --scenarios champion,s1,s14 --reports /tmp/gr-e2e-plan
```

Live execution is intentionally explicit and must use the persistent v1
deployment checkout and an external state directory:

```bash
python3 shared/scripts/e2e/driver.py --cloud aws \
  --repo "$GR_REPO" --checkout "$GR_REPO" --commit "$SHA" \
  --state-dir "$GENIERAILS_STATE_DIR" --reports "$ARTIFACTS" \
  --scenarios champion --confirm-live
```

The checkout must be customer-shaped, that is, it must commit `envs/`. The
upstream GenieRails `.gitignore` ignores `envs/`, so in a plain clone the
driver's local merge (`git add -A -- envs`) would fail. Before anything runs
(no log, git switch, snapshot or `make`), the driver checks, with read-only
git, that:

- the checkout is already at `--commit` (check it out first) and clean, so
  git's own ignore rules (every `.gitignore`, `.git/info/exclude`,
  `core.excludesFile`) are evaluated against exactly that tree;
- `<cloud>/.gitignore` has the block `make setup GITIGNORE=customer` writes;
- `<cloud>/envs/dev/env.auto.tfvars` and `<cloud>/envs/prod/env.auto.tfvars`
  are committed as regular files (mode `100644`/`100755`) in real
  directories, with no symlink anywhere in the path;
- `git check-ignore` ignores neither file.

If any check fails it exits non-zero with what is missing and the fix, and
runs nothing. Prepare the checkout once:

```bash
cd aws
make setup ENV=dev GITIGNORE=customer
make setup ENV=prod GITIGNORE=customer
git add .gitignore envs && git commit -m "Commit envs/ (customer repo)"
```

Then pass that commit (the checked-out `HEAD`) as `--commit`. The upstream root `.gitignore` is left
as it is: `<cloud>/.gitignore` re-includes `envs/`.

Live runs require an explicit 40-hex commit and persistent checkout. Before
each scenario and each step the driver verifies HEAD is the pinned commit
(`git checkout --force --detach <commit>` at scenario start). Applying targets
(`rehearse`, `release-plan`, `release`, `maintain`, operations) run only on a
clean tree; authoring targets (`setup`, `generate`, `promote-to`, `ungovern`)
may change `envs/`, and the scenario's next step commits that on a
`genierails-e2e/<scenario>-*` branch and merges it with `--no-ff` into the
driver's own main. That main lives in a bare repo under `<reports>/e2e-remote.git`
(git remote `genierails-e2e`), and every `make` runs with
`RELEASE_REMOTE=genierails-e2e`, so release-plan's "merged on main" rule holds
without pushing to `origin`. The driver removes that remote when the run ends,
whether it passed, failed or was interrupted (the bare repo stays with the
reports). It refuses to reuse a `genierails-e2e` remote that points anywhere
else. If an earlier run was killed before its cleanup, check that the remote's
URL (`git remote get-url genierails-e2e`) is an old `<reports>/e2e-remote.git`,
then clear it with `git remote remove genierails-e2e`.

Everything a scenario needs is checked before it touches anything. A missing
public target or a human gate without a TTY makes the whole scenario `NOT RUN`. An
unsupplied `--value` makes it `ERROR`. Either way no snapshot, git switch or
`make` runs. Placeholder text (`<...>`, `{...}`) and empty `KEY=` arguments
never reach `make`. A refusal step passes only with a non-zero exit, the exact
diagnostic the target prints, and identical before/after snapshots. Any other
failure is `ERROR`. S13 runs every refusal command with `CI=true` and without
`GENIERAILS_ALLOW_CI_APPLY`.

Scenario values: `agent_a`, `wrong_agent`, `table` (covered), `uncovered_table`,
`treatment`, `ack_entry` (an exact `prod:<kind>:...` line), `manifest`,
`tampered_manifest`, and the 40-hex commits `u`, `o`, `r`, `r_plus_1`, `x`, `y`.
The champion also needs `dev_agent_id` (dev agent A's Genie ID) and
`acl_groups` (who may run A, comma-separated): before `make generate` it writes
`envs/dev/agents/<agent_a>.auto.tfvars` with them, as the walkthrough's agent
template does, so `agent_a` must be A's Terraform key (lowercase letters, digits
and `_`). An existing file that says otherwise is refused. Its prod `GrantDelta`
then requires CAN_RUN on A for exactly those groups.
The typed checks below add: champion `acl_groups`, `dev_tables`, `prod_tables`;
S2 `mask_column`; S3 `new_table_principals`, `dev_table`, `prod_table`; S4
`new_column`; S5 `secret_column`; and `agent_a` for champion, S3 and S16. S6's
`reader_principal` and `reader_table` come from `ack_entry`
(`prod:reader:<cat.sch.table>:<principal>`). A comma-separated value
(`acl_groups=a,b`) stands for every combination.

### Typed checks

A step's `assertions=` and a scenario's `checks=` hold typed checks from a
small vocabulary, never free text. Each has an evaluator that runs in live runs;
a step's checks fail that step, a scenario's checks fail the scenario after its
final snapshot. `--dry-run` lists them and exits non-zero if any check has no
evaluator or is malformed (unknown env or component, wildcards in a grant, an
unknown level). Their `{placeholders}` are required values.

| Check | Passes when |
| --- | --- |
| `GrantDelta(env, added=, may_add=, revoked=)` | Grants added since the baseline are exactly `added` plus a subset of `may_add`, and those revoked are exactly `revoked`. |
| `AccessLevel(env, target, principal, level)` | The step's verify-access result shows `principal` (`*` = every one checked) at `raw`, `partial`, `full`, `redacted` or `masked` on every checked column under `target`. |
| `AccessVerified(env, target)` | That result has PASS for every check under `target`, each principal at its expected tier. `ADOPTED_COLUMNS` means every column migrate-plan adopted. |
| `DiffLimitedTo(env, component, selectors)` | Every changed `uc` resource, `agents` space, `receipts` or `state_serials` path matches a selector. |
| `Unchanged(env, component, selectors=("*",))` | Nothing matching the selectors changed. |
| `AgentIdUnchanged(env, agent)` | The agent's Genie space ID is the baseline's, so it was not recreated and its conversations remain. |

A grant is `(principal, privilege, securable)`: a table, schema or catalog, or
`agent:<key>` for Genie CAN_RUN. `USE_CATALOG`/`USE_SCHEMA` on a table mean its
catalog/schema. Every protection check includes UC grants and non-inherited
CAN_RUN. After each applying step, each release phase and at the scenario end, a
new grant that no check of the scenario declares in `added` or `may_add` fails,
including in S8/S9, which may otherwise weaken protection. The restore and
final checks allow no new grant. Example:

```python
_scenario("s6", ..., checks=(
    GrantDelta("dev"),
    GrantDelta("prod", added=(("{reader_principal}", "SELECT", "{reader_table}"),),
               may_add=_business_grants("{reader_principal}", "{reader_table}")),
))
```

Access checks read `<cloud>/envs/<env>/generated/.verify_access.json`, the file
`release`, `rehearse` and `verify-access` write. A file the step did not rewrite
is stale and fails the check. The file's `checks` list gives, per column and
principal, the expected tier and what the run proved (`raw`, `mismatch`,
`fail-closed` or `unproven` when it did not prove the expected tier).
`redacted` means `full` through a never-raw (`unmapped_redact`) mask.

Snapshots read `<cloud>/envs/<env>/auth.auto.tfvars`
(`databricks_workspace_host`, `databricks_client_id`,
`databricks_client_secret`) and connect with OAuth M2M while `DATABRICKS_*`,
`ARM_*` and `AZURE_*` are hidden from the SDK. A client that resolves any other
host is refused. A failed query, an unreadable catalog, or an unreadable state
file is an error, never empty evidence. Prod agent IDs are also read from
`.genie_space_id_<key>`. Local Terraform serials are recorded; an environment
configured only with remote state is recorded explicitly as
`remote, not snapshotted`, never as an empty inventory.

Human gates need an interactive TTY and the typed scenario ID. A closed or
piped stdin aborts the run, and the scenario is `NOT RUN`. A wrong token is
`ERROR`. Ctrl-C, SIGTERM and SIGHUP stop the run as `interrupted`. In every
case the scenario's `finally` restores and writes a partial report. The restore
first resets the checkout to the commit. After any applying step it then
releases the commit's tree again as a forward fix on the driver's main
(`rehearse`, bounded `release-plan`/`release MANIFEST=` cycles, `maintain`). A
fresh immutable manifest is used for every staged release phase. Successful
release plans must leave the snapshot unchanged, and successful applying steps
are checked immediately for weaker protection. This catches protection loss at
command boundaries; no snapshot driver can observe a transient exposure made
and repaired entirely within one command. As an additional guard, deterministic
applying steps fail if `import_existing.sh` logs either its literal
`Cleared tag gr_treatment on ...` cleanup or `; clearing tag` warning. A test
(and `--dry-run`, under "would not run") checks that `import_existing.sh` still
prints those lines, so a rewording fails CI instead of silently passing. The
guard only sees what the log says: a wholly quiet internal exposure still cannot
be observed, so the engine must preserve its internal masks-before-access
ordering. The driver then
re-snapshots dev and prod. A column that lost a tag, a removed mask or policy, a
newly exempted or uncovered principal, or a new grant or CAN_RUN compared with the
scenario's baseline is `RESTORE FAILED`. `<reports>/final-protection.json`
repeats that check against the run's first baseline. Logs and reports redact
secret-named keys, every value in the cloud's auth files, `Authorization`,
`Bearer` and `*_TOKEN=` values. Reports and state must be outside the checkout.
