# Deterministic mask library reference

`mask_library.json` maps all 93 Databricks `class.*` classifications to fixed,
caller-independent partial and full versions. `identifier_partial_default` is
the sole library-level product switch: it is currently `hmac_sha256` and may be
changed to `redacted` without editing every identifier mapping.

| Treatment | Classes | Partial | Full | Types |
|---|---:|---|---|---|
| Financial card | 1 | last 4 | redacted | STRING |
| Card secrets / expiry | 4 | redacted | redacted | STRING, DATE, TIMESTAMP |
| Financial account | 5 | last 4 | redacted | STRING |
| Government, health, vehicle and device identifier | 58 | keyed HMAC-SHA-256 | redacted | STRING, numeric |
| Email / phone / name | 3 | email partial / last 4 / initials | redacted | STRING |
| Date / age / credit score / compensation | 5 | year / 10-point band / 50-point band / rounded | NULL | typed |
| IP / MAC / URL | 3 | network / vendor / domain | redacted | STRING |
| Location | 1 | STRING redacted; numeric 1 decimal place | typed redacted | STRING, numeric |
| Sensitive attributes | 12 | redacted | redacted | STRING |
| Secret | 1 | redacted | redacted | all supported types |

`card_security_code`, `card_pin`, `card_track_data`, and `secret` are never raw.
When several class tags occur on one column, the strongest partial treatment
wins (`raw < partial < keyed hash < redacted/NULL`). Unsupported types receive
the full version. Numeric identifiers are converted to canonical decimal text
before hashing.

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
