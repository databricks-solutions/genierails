# Effective-Access Verification

Verify that GenieRails governance actually **takes effect** — not just that the
masks, tags, and policies exist in the metastore, but that a real principal
running a real query gets back the values its access tier is entitled to.

This implements roadmap item #5, *Stronger Integration Test Assertions*
([`roadmap.md`](roadmap.md)).

## What it checks — by effect, not by existence

The existing integration checks in `scripts/setup_test_data.py` query
`information_schema.column_masks` / `column_tags` to confirm governance objects
were created. That proves the plumbing is in place; it does **not** prove the
plumbing works. A mask that is attached but bound to the wrong function, or a
row filter whose `WHEN` clause never matches, both pass an existence check while
silently leaking data.

`verify_effective_access.py` closes that gap by comparing **query output across
access tiers**:

| Check | Assertion |
|---|---|
| **Column mask** | A lower-tier principal sees a **masked** value while a higher-tier principal sees the **raw** value for the *same row*. Equal values = the mask did not take effect (a leak → FAIL). |
| **Row filter** | A restricted principal gets back **fewer rows** than an unrestricted principal. Equal/greater counts = the filter is not restricting (FAIL). |

The comparison is done by *effect*: rather than assuming a mask function's exact
output, the tool pairs each row (by a primary-key column) between a masked and an
unmasked principal and asserts the masked tier's value differs from the **raw**
value the unmasked tier sees.

### Inconclusive never passes

A verification gate must never report success for something it did not actually
prove, so the tool has exactly one passing outcome (**PASS**) and treats every
non-conclusive outcome as blocking:

- **FAIL** — a violation was proven (a mask leaked the raw value, a filter did
  not restrict, unmasked principals disagreed on the raw value, or a query
  failed / a principal could not be provisioned).
- **INCONCLUSIVE** — the check could not be verified (no baseline rows, no
  overlapping rows, an all-NULL/empty sample, or a restricted principal with no
  collected count). This is kept distinct from FAIL only for diagnostics.

Both FAIL and INCONCLUSIVE make `make verify-access` exit non-zero. A run that
derives **zero** checks also exits non-zero — verifying nothing is not success.
To keep it honest, a column-mask PASS additionally requires that the unmasked
principals **agree** on the raw value (a disagreement means one is not really
unmasked) and that at least one **non-NULL/non-empty** row was actually
compared for every masked principal.

## Why dedicated per-tier test principals

Databricks does **not** offer general per-user query impersonation. There is no
supported "run this `SELECT` as user `alice`" from an admin context — Unity
Catalog evaluates FGAC policies against the identity that actually issues the
query.

The supported mechanism is therefore a set of **dedicated test principals**:

- one **service principal per access tier**,
- each added as a **member of that tier's account group** (e.g. `Junior_Analyst`,
  `Compliance_Officer`),
- each authenticating with **its own OAuth credentials** (`client_id` /
  `client_secret`) so it runs the query as itself.

Because each SP carries only its tier's group membership, Unity Catalog applies
exactly the masks and row filters that tier should get, and the values it reads
back are ground truth for that tier. An account-admin baseline (the credentials
already in `auth.auto.tfvars`) provides the raw-value reference for masks that
target a specific group.

> **Limitation — the all-users case.** For a mask whose `to_principals` is the
> built-in `account users` group (everyone) with an `except_principals` carve-out,
> the admin baseline is *also* a member of `account users` and would see the
> masked value. In that case only the excepted principals are a valid raw
> baseline, and the tool derives the check accordingly.

## Running it

### Dry run — no workspace needed

Print the checks the tool derives from your config (safe in CI, no cluster):

```bash
make verify-access-spec ENV=dev
# or directly:
python3 verify_effective_access.py \
  --from-tfvars envs/dev/data_access/abac.auto.tfvars \
  --account-tfvars envs/account/abac.auto.tfvars \
  --print-spec
```

### Live verification — against a deployed environment

Run **after `make apply`**, so the governance is deployed:

```bash
make verify-access ENV=dev VERIFY_KEY_COLUMN=customer_id
```

Options (see `Makefile.shared`):

| Variable | Meaning |
|---|---|
| `ENV=<env>` | Workspace env whose `data_access` config to verify (default `dev`) |
| `VERIFY_KEY_COLUMN=<c>` | Primary-key column used to pair rows across principals |
| `VERIFY_SPEC=<file.json>` | Explicit JSON spec instead of `--from-tfvars` derivation |
| `WAREHOUSE_ID=<id>` | Pin a specific SQL warehouse |
| `KEEP_PRINCIPALS=1` | Leave the provisioned test principals in place (debugging) |

The live path is **guarded**: it runs only when both `--live` is passed *and*
`GENIERAILS_LIVE_VERIFY=1` is set (the `make verify-access` target sets both).
The guard is enforced at construction of the live verifier **and re-checked
before every network call**, so no live call can happen without the flag. It
requires account-admin credentials (to create service principals and manage
group membership) and a running SQL warehouse. Test principals are deleted
automatically at the end unless `KEEP_PRINCIPALS=1`.

Exit code is non-zero unless **every** check PASSes (see *Inconclusive never
passes* above), so it drops into a CI pipeline.

### Explicit spec (`--spec`)

When the derived spec doesn't match your data (e.g. a different key column per
table, or you want to hand-pick tiers), provide a JSON spec:

```json
{
  "column_masks": [
    {
      "table": "dev_fin.finance.customers",
      "column": "ssn",
      "key_column": "customer_id",
      "masked_principals": ["Junior_Analyst"],
      "unmasked_principals": ["Compliance_Officer"],
      "policy_name": "mask_pii_ssn"
    }
  ],
  "row_filters": [
    {
      "table": "dev_fin.finance.transactions",
      "restricted_principals": ["Junior_Analyst", "Senior_Analyst"],
      "unrestricted_principals": ["Compliance_Officer"],
      "policy_name": "filter_aml_clearance"
    }
  ]
}
```

## How it is tested

The **comparison and spec-derivation logic is pure** (no Databricks) and is
covered by unit tests in `tests/test_verify_effective_access.py`, which feed
mocked query results (masked vs. raw values, row counts) through the evaluators
and assert PASS/FAIL/SKIP outcomes. These run in the standard `make test-unit` /
`pytest shared/tests/` gate with no cluster.

The **live layer** (`EffectiveAccessVerifier`, `verify_effective_access_live`)
is exercised only against a real workspace and is guarded so unit runs never
reach it — a unit test asserts the guard raises when `GENIERAILS_LIVE_VERIFY`
is unset.
