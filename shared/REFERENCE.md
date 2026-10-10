# Deterministic mask library reference

`mask_library.json` maps all 93 Databricks `class.*` classifications to fixed,
caller-independent partial and full versions. `identifier_partial_default` is
the sole library-level product switch: it is currently `hmac_sha256` and may be
changed to `redacted` without editing every identifier mapping.

Treatment names are the shared `treatment_versions` vocabulary, so each one
can be configured on its own (for example `identifier = { partial = "redacted" }`).

| Treatment | Classes | Partial | Full | Types |
|---|---:|---|---|---|
| `card_last4` | 1 | last 4 | redacted | STRING |
| `card_security_code`, `card_pin`, `card_track_data` | 1 each | redacted (never raw) | redacted | STRING |
| `account_last4` | 5 | last 4 | redacted | STRING |
| `ssn`, `tfn_partial`, `medicare_partial`, `aadhaar_partial` | 2, 1, 1, 1 | keyed HMAC-SHA-256 | redacted | STRING, numeric |
| `identifier` (other government, health, vehicle and device IDs) | 53 | keyed HMAC-SHA-256 | redacted | STRING, numeric |
| `email_partial` / `phone_partial` / `name_partial` | 1 each | email partial / last 4 / initials | redacted | STRING |
| `date_year` | 2 | year | NULL | DATE, TIMESTAMP, TIMESTAMP_NTZ |
| `age` / `credit_score` / `compensation_redacted` | 1 each | 10-point band / 50-point band / rounded | NULL | numeric |
| `ip_address` / `mac_address` / `url` | 1 each | IPv4 /24 network (IPv6 redacted) / vendor / domain | redacted | STRING |
| `location` | 1 | STRING redacted; numeric 1 decimal place | typed redacted | STRING, numeric |
| `redact` (card expiry and sensitive attributes) | 13 | redacted | redacted | STRING, DATE, TIMESTAMP, TIMESTAMP_NTZ |
| `secret` | 1 | redacted (never raw) | redacted | all supported types |

`card_security_code`, `card_pin`, `card_track_data`, and `secret` are never raw:
their classes map to treatments of the same names, which tier 1 also sees in
the full version. When several class tags occur on one column, the strongest
partial treatment wins (`raw < partial < keyed hash < redacted/NULL`); a tie
goes to the greater treatment name, so tag order never matters. Unsupported
types receive the full version. Numeric identifiers are converted to canonical
decimal text before hashing; numeric hashes need at least 19 integer digits
(`BIGINT`, or `DECIMAL(p, s)` with `p - s >= 19`), otherwise the typed full
version is used.

`scripts/live_mask_library.py` checks every shipped SQL body against exact
expected values on a warehouse (`DATABRICKS_LIVE_TESTS=1`); offline tests check
the Python reference against the same values. Results are rendered in
`America/Los_Angeles` per statement, because the Statement Execution API does
not keep session settings between calls.

The keyed hash reads its UC secret once at Python-module initialization. The
secret is the 64-character hexadecimal key text encoded as UTF-8 (not decoded
hex bytes), consistently in the reference and UDF. It is not declared
deterministic. Step 5 will provision the secret automatically, enforce the
explicit `hash_fallback = "redact"` capability gate, and compare scratch probe
hashes between dev and prod without writing them into a real environment's
`generated/` directory. Metastore admins and principals holding `MANAGE` on the governance
schema can grant themselves `READ SECRET`; this is a residual platform risk.

Measured on the AWS dev serverless Pro warehouse: a query touching a hashed
column has about **8 seconds fixed latency**, roughly unchanged between 10,000
and 100,000 rows. Tier 1 raw and tier 3 SQL-only masks do not pay this cost.
