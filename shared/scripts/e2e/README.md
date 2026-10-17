# Deterministic-governance E2E driver

The driver encodes the champion flow from
`shared/docs/design/deterministic-governance.md` sections 9, 12, 13 and 17,
plan scenarios S1–S16, cross-cutting
manifest/concurrency checks, the report-only migration operation, the
migration cutover, and S17 (`policy-ops`, staged policy operations). A scenario that is expected to fail until a fix lands
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
`tampered_manifest`, `failing_verify_spec` (S17; an absolute path), and the 40-hex commits `u`, `o`, `r`, `r_plus_1`, `x`, `y`.

S17 (`policy-ops`) covers the day-2 policy operations in design section 17 and
`docs/deterministic-policies.md` with public targets only. It starts with
`make policy-status` in dev and prod, which must show an active generation
and nothing unfinished or waiting to retire. A reviewed mask-only dev change
(no tier move, no new `table_readers`) is authored, regenerated and merged.
Then the steps are:

1. **Failure.** `make rehearse ENV=dev VERIFY_SPEC=<failing_verify_spec>`
   fails at `verify-created`. The injection is the documented `verify-access`
   option, which reaches the staged verification as a command-line make
   variable, so no product backdoor is involved. The spec is a reviewed JSON
   file outside the checkout. Its `column_masks` check lists a tier-1 principal
   under `masked_principals`, so the read-only verification can't pass after
   `create`. The step must print `ERROR: policy phase verify-created failed`.
   Every tag, grant and earlier policy must be unchanged, no mask may be lost,
   and a new policy may exist only if it matches no value a column carries
   (nothing switched).
2. **Status.** `make policy-status ENV=dev` must report the unfinished change,
   `failed at verify-created`, and the previous protection with no column
   retagged.
3. **Abandon.** `make policy-abandon ENV=dev` must print `Abandoned
   generation ...`. Dev and prod protection must then equal the checkpoint
   recorded before step 1: same tags, grants and policies. Unreferenced
   functions left by `create` are not protection.
4. **Success.** `make rehearse ENV=dev` must run `create -> verify-created ->
   switch -> verify-switched -> grant`. `policy-status` must then show the
   superseded dev generation as confirmed.
5. **Promotion and retire.** Then come the promotion steps of the day-2
   sequence (promote, merge, prod check, `CHECK=1`, rehearse the merge) and
   the prod release, which needs at least two `release-plan`/`release
   MANIFEST=` pairs, each with a fresh manifest (one approval per changing
   phase). A later `make maintain ENV=prod` must print `retired <generation>`,
   and prod `policy-status` must show nothing waiting to retire.

Every applying step is checked for weaker protection, as in the other scenarios.

Output assertions (`assertions` must match, `forbidden` must not) apply to a
step that otherwise passed. `protection_checkpoint` records dev and prod
protection under a name, and a later `protection_matches` step compares
against it. A positive `make apply-account` scenario is not encoded yet:
`apply-account` has the CI guard and the account lock, but none of the
section 17 provenance (clean `origin/main` HEAD, saved account plan and
manifest, account receipt) it would assert. S13 keeps its CI refusal.

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
newly exempted or uncovered principal, or a new grant compared with the
scenario's baseline is `RESTORE FAILED`. `<reports>/final-protection.json`
repeats that check against the run's first baseline. Logs and reports redact
secret-named keys, every value in the cloud's auth files, `Authorization`,
`Bearer` and `*_TOKEN=` values. Reports and state must be outside the checkout.
