# Deterministic tier policies (rollout step 5)

With `governance_mode = "deterministic"`, governance is computed only from
`class.*` tags, column types and reviewed config (`access_tier_groups`,
`raw_exempt_principals`, `treatment_versions`, `column_overrides`). No model
output shapes a mask. `make generate` builds no prompt and calls no model, so
it needs no model endpoint (design decision 5): the column masks and
`gr_treatment` tags come from the tags and the mask library, the Genie config
from the Genie API import, and the rest (groups, row filters, table tags) from
the reviewed draft in `generated/`. Legacy envs are unchanged: none of this
runs for them.

Code: `governance_policies.py` (pure), `generate_abac.write_deterministic_model`,
`scripts/derive_assignments.py`, `scripts/external_mask_inventory.py`,
`scripts/policy_generations.py`, `scripts/staged_policy_apply.py`,
`scripts/staged_policy_verify.py`.

## What a governed column gets

- The strictest of its classes through the mask library (`mask_library.json`).
- Any never-raw class (`card_security_code`, `card_pin`, `card_track_data`,
  `secret`) makes the column never raw.
- Any class the library doesn't map makes it the fail-safe `unmapped_redact`:
  never raw, redacted for every tier. A `gr.classification_unmapped` marker is
  written too, so the coverage check keeps blocking new access until a
  mapping is promoted.
- Complex column types (ARRAY, MAP, STRUCT) and `tier_access_overrides` are
  refused rather than guessed.

## Names

| Object | Name |
|---|---|
| Treatment value (`gr_treatment`) | `<treatment>__<type>__<generation>`, e.g. `email_partial__string__3f9a1c2b` |
| Functions | `gr_mask_<value>_partial`, `gr_mask_<value>_full`, typed exactly for the column (`DECIMAL(10,2)` in, `DECIMAL(10,2)` out), in the catalog's first governed schema |
| Policies | `gr_mask_<catalog>_<value>_partial`, `gr_mask_<catalog>_<value>_full` |

Unity Catalog has no function overloading, so the column type is part of the
value. `<generation>` hashes the treatment, type and both SQL bodies: changing
a mask creates new names and never replaces a live function or policy.
Principals are not part of it, so a tier move updates a policy in place.

## Policies per value

| Policy | To | Except |
|---|---|---|
| partial (only with a middle tier, never for never-raw) | tier-2 groups | tier 1, `raw_exempt_principals`, deployer SP |
| full | `account users` | tiers 1 and 2, `raw_exempt_principals`, deployer SP; **nobody** for never-raw |

Every principal gets at most one mask: the most privileged tier wins, and
anyone outside the tiers gets the full version.

## Governance only

`uc_tables` governs and never grants. `SELECT` (with its `USE_CATALOG` and
`USE_SCHEMA`) comes only from `table_readers`, and nothing targets `account
users`; without `table_readers` GenieRails grants nothing. With no agents, the
workspace layer is skipped, so GenieRails creates nothing there (no group
assignment, entitlement, warehouse or Genie resource). See
[access.md](access.md) for `table_readers`, `acl_groups`, the live agent
coverage check that gates `CAN_RUN`, and `group_map`.

## External masks

Before every rehearse, release and maintain (and plan/apply through the
exposure preflight), `scripts/external_mask_inventory.py` stops the run when a
governed table carries a column-mask policy GenieRails didn't create
(catalog, schema or table level, from the policies API) or a governed column
carries a table-attached mask (`information_schema.column_masks`). Policies of
any retained generation are recognised by name and function. GenieRails
supports only governance it generates (design decision 9).

## Staged policy changes (rollout step 6)

A deterministic env's data_access layer is never applied in one Terraform
apply. `make apply`, `apply-governance`, `rehearse`, `release` and `maintain`
apply it through `scripts/staged_policy_apply.py`, under the env lock (reused
when the enclosing `release` or `maintain` holds it). Legacy envs are never
staged: their data_access apply is unchanged and the controller changes
nothing for them.

| Phase | What happens |
|---|---|
| `create` | new functions and policies; retained policies keep their principals; no column is retagged and nothing is removed |
| `verify-created` | the live policies, functions and treatment tags equal the render (policies API, `include_inherited` on every governed table); verify-access proves every column still gives each tier its **old** output (old tier settings) |
| `tighten`, `verify-tightened` | tier moves only: each moved policy takes the union of its old and new `TO` and the intersection of its `EXCEPT`, so nobody masked before or after is unmasked; only principals being moved can briefly hit `MULTIPLE_MASKS` |
| `switch` | columns are retagged and principals take their new values |
| `verify-switched` | live check again, and verify-access with the **new** tier settings |
| `retire`, `verify-retired`, `drop-functions` | only for a generation an **earlier** release made obsolete and a successful release then confirmed: its policies leave, then the functions only it used are dropped once no live policy calls them |
| `grant` | last, in every run: new `SELECT` from `table_readers`. Every earlier phase plans with new grants withheld (grants already applied stay), so access opens only after its protection is applied and verified; this plan may change no policy or tag |

Before each apply the hook checks its render: every tagged value has a
policy, and nobody gets two masks except principals a tier move is moving.
Each plan is refused if it replaces anything or deletes a policy or tag the
phase doesn't remove, or creates business access before the switch. The
data_access root refuses, at plan time, any change to a live deterministic mask
(removal, new principals, a new function, a retag) outside the controller's
phases (`policy_stage`), so a raw `terraform apply` can't make one in one shot.

**Journal.** `<env>/data_access/.policy_generations/journal.json`, next to the
layer's state (with `GENIERAILS_STATE_DIR` set, the same path under
`$GENIERAILS_STATE_DIR/<cloud>/<env>/records/`, so a fresh checkout sees it),
records the active generation, the one waiting to retire and
any unfinished transition with its completed phases. Writes are atomic.

- A failed or crashed run is **resumed** by re-running the same command on the
  same commit. A run whose generated config differs from the unfinished one is
  **refused** before anything is applied.
- A failure in `tighten`, `switch` or their verification **restores** the
  previous protection at once (through `tighten` again when principals may
  already be moved), so columns go back to their old value and principals;
  newly governed columns keep their new mask.
- A failure in `create`, `retire` or `drop-functions` leaves everything live;
  the journal keeps the generation waiting to retire.
- The generation a run switched away from is retired only by a **later** run,
  after the release that switched (`rehearse`, `release`, `maintain`,
  `apply`, `apply-governance`) completed and confirmed it
  (`staged_policy_apply.py confirm`). A new policy change waits until the
  previous one is confirmed.
- A state that differs from the journal (applied outside the controller, or a
  stale journal) is refused. With no journal, a state that exactly matches
  the generated config is adopted; any other is refused.
- `make policy-status ENV=<env>` prints the journal. `make policy-abandon
  ENV=<env>` drops an unfinished change that never retagged a column or moved
  a principal, so a corrected config can start a new one.
- `make plan` prints the phases the next apply will run, then plans the end
  state.

In `rehearse`, `apply` and `maintain` these phases run inside the applying
command. A prod release is two-phase (rollout step 9, [pipelines.md](pipelines.md#release-gating-deterministic-envs)):
`make release-plan` runs the controller's `plan --out-dir`, which saves the
plan of each remaining apply phase in order (a phase that changes nothing is
recorded and planning goes on) and stops after the first phase that changes
something, with the verifications right after it; `make release
MANIFEST=...` runs `apply-saved`, which applies exactly those saved plans
(a saved plan that changes nothing isn't applied) and runs those
verifications. It refuses when the journal, the generated config or a stage's
render moved since the plan. A change that replaces policies or moves tiers
is therefore one manifest and approval per changing phase; a release with no
policy change is one manifest. A failed tighten or switch still restores the
previous protection at once.

Hook environment (also `GENIERAILS_POLICY_PLAN_OUT`, `GENIERAILS_POLICY_PLAN_BASIS`
and `GENIERAILS_POLICY_SAVED_PLAN` for the saved-plan modes; see
`scripts/policy_generations.py`):

| Variable | Meaning |
|---|---|
| `GENIERAILS_ENV_DIR` | the env directory (required) |
| `GENIERAILS_ENV_NAME` | the env name (defaults to the directory name) |
| `GENIERAILS_POLICY_FINGERPRINT` | must equal `python3 scripts/policy_generations.py fingerprint --env-dir <env>` |
| `GENIERAILS_OLD_POLICY_FINGERPRINT` | the active generation, kept live |
| `GENIERAILS_RETIRE_FINGERPRINT` | a generation an earlier release made obsolete, kept live until `retire` |
| `GENIERAILS_POLICY_KEEP_NEW_TAGS` | `1` once a switch was attempted: newly governed columns keep their value |

Each applied generation is snapshotted in
`<env>/data_access/.policy_generations/<fingerprint>.json` with the tier
settings it was verified with, and the last applied stage in `stage.json`.
A hook refuses to run when a live generation has no snapshot.
The account tag sync never removes a live deterministic value; the caller runs
`sync-tags` and `wait-tag-policies` before `create`.

## Tag-policy values

Rehearse, release and apply in a deterministic env only **add** tag-policy
values (`sync_tag_policies.py --mode add-only`). Terraform ignores the values,
so an empty account plan doesn't prove the REST update is safe: when a live
value is no longer desired, the sync writes nothing, lists the values it would
remove and fails, pointing at `make apply-account`. Only `make apply-account`
(CI guard, account lock) runs `--mode replace`, which removes them. While any
env of the checkout (`envs/*/env.auto.tfvars`) is deterministic,
`make apply ENV=account` runs `make apply-account`, and a bare
`make sync-tags ENV=account` is add-only. Legacy-only checkouts keep `replace`,
as before.

Migrating a legacy env: [legacy-migration.md](legacy-migration.md).
