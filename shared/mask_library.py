"""Deterministic, caller-independent mask-library reference implementation."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import ipaddress
import json
import re
import unicodedata
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlsplit

LIBRARY_PATH = Path(__file__).with_name("mask_library.json")
REDACTED = "[REDACTED]"
NUMERIC_TYPES = frozenset({"BYTE", "TINYINT", "SHORT", "SMALLINT", "INT", "INTEGER", "LONG", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"})


def load_library(path: Path = LIBRARY_PATH) -> dict:
    return json.loads(path.read_text())


def type_family(sql_type: str) -> str:
    value = sql_type.upper().split("(", 1)[0].strip()
    if value in {"CHAR", "VARCHAR"}:
        return "STRING"
    if value == "TIMESTAMP_NTZ":
        return "TIMESTAMP"
    return "NUMERIC" if value in NUMERIC_TYPES else value


def normalize_identifier(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value).strip()).upper()
    return re.sub(r"[\s-]+", "", text)


def canonical_decimal(value: object) -> str | None:
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite():
        return None
    if number == 0:
        return "0"
    rendered = format(number.normalize(), "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def keyed_hash(value: object, key: bytes, *, numeric: bool = False) -> str | int | None:
    if value is None:
        return None
    normalized = canonical_decimal(value) if numeric else normalize_identifier(value)
    if normalized is None:
        return None
    digest = hmac.new(key, normalized.encode("utf-8"), hashlib.sha256).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1) if numeric else digest.hex()


def _finite_decimal(value: object) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def apply_version(version: str, value: object, sql_type: str, *, key: bytes | None = None) -> object:
    """Apply a named library version. Malformed/unsupported inputs get its full value."""
    if value is None:
        return None
    family = type_family(sql_type)
    if version == "raw":
        return value
    if version in {"redacted", "null"}:
        return REDACTED if version == "redacted" and family == "STRING" else None
    if version == "hmac_sha256":
        if key is None:
            raise ValueError("hmac_sha256 requires a key")
        return keyed_hash(value, key, numeric=family == "NUMERIC") if family in {"STRING", "NUMERIC"} else None
    if version == "last4" and family == "STRING":
        text = str(value).strip()
        compact = re.sub(r"[\s-]+", "", text)
        return "*" * (len(compact) - 4) + compact[-4:] if len(compact) > 4 else REDACTED
    if version == "email_partial" and family == "STRING":
        text = str(value).strip()
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text):
            return REDACTED
        local, domain = text.rsplit("@", 1)
        return local[0] + "***@" + domain.lower()
    if version == "initials" and family == "STRING":
        words = re.findall(r"[^\W\d_]+", str(value), flags=re.UNICODE)
        return "".join(word[0].upper() for word in words) if len(words) > 1 else REDACTED
    if version == "year":
        if family == "DATE" and isinstance(value, dt.date):
            return dt.date(value.year, 1, 1)
        if family == "TIMESTAMP" and isinstance(value, dt.datetime):
            aware = value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
            year = aware.astimezone(dt.timezone.utc).year
            return dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
        return None
    if version in {"age_band_10", "credit_score_band_50"} and family == "NUMERIC":
        number = _finite_decimal(value)
        if number is None or number < 0:
            return None
        width = Decimal(10 if version == "age_band_10" else 50)
        lower = (number // width) * width
        return int(lower)
    if version in {"rounded", "location_1dp"} and family == "NUMERIC":
        number = _finite_decimal(value)
        if number is None:
            return None
        quantum = Decimal("1E3") if version == "rounded" else Decimal("0.1")
        return number.quantize(quantum, rounding=ROUND_HALF_UP)
    if version == "ip_network" and family == "STRING":
        try:
            address = ipaddress.ip_address(str(value).strip())
            prefix = 24 if address.version == 4 else 64
            return str(ipaddress.ip_network(f"{address}/{prefix}", strict=False))
        except ValueError:
            return REDACTED
    if version == "mac_vendor" and family == "STRING":
        chunks = re.split(r"[:-]", str(value).strip())
        return ":".join(part.upper() for part in chunks[:3]) + ":**:**:**" if len(chunks) == 6 and all(re.fullmatch(r"[0-9A-Fa-f]{2}", p) for p in chunks) else REDACTED
    if version == "url_domain" and family == "STRING":
        try:
            parsed = urlsplit(str(value).strip())
            return parsed.hostname.lower() if parsed.scheme in {"http", "https"} and parsed.hostname else REDACTED
        except ValueError:
            return REDACTED
    if version == "prefix_3" and family == "STRING":
        text = str(value).strip()
        return text[:3] + "***" if len(text) > 3 else REDACTED
    return REDACTED if family == "STRING" else None


def resolve_class(classes: list[str], sql_type: str, library: dict | None = None) -> tuple[str, str, bool]:
    """Resolve several class tags with G17 strictest-wins semantics."""
    data = library or load_library()
    family = type_family(sql_type)
    order = {name: i for i, name in enumerate(data["protection_order"])}
    candidates = []
    for treatment_name, treatment in data["treatments"].items():
        for class_name in classes:
            if class_name.removeprefix("class.") not in treatment["classes"]:
                continue
            if family not in treatment["types"]:
                partial = treatment["full"]
            else:
                partial = treatment.get("partial_by_type", {}).get(family, treatment.get("partial"))
            if partial == "$identifier_partial_default":
                partial = data["identifier_partial_default"]
            candidates.append((order["hmac_sha256"] if partial == "hmac_sha256" else order["redacted"] if partial in {"redacted", "null"} else order["partial"], treatment_name, partial, treatment["full"], bool(treatment.get("never_raw"))))
    if not candidates:
        raise KeyError(f"no mask-library mapping for {classes!r}")
    never_raw = any(candidate[4] for candidate in candidates)
    _, _, partial, full, _ = max(candidates, key=lambda candidate: candidate[0])
    return partial, full, never_raw


def resolve_hash_capability(partial: str, full: str, *, available: bool, fallback: str | None) -> str:
    """Fail closed only when a selected treatment actually needs keyed hashing."""
    if partial != "hmac_sha256" or available:
        return partial
    if fallback == "redact":
        return full
    raise RuntimeError(
        "keyed hash requires Unity Catalog secrets and Python UDFs; "
        "set hash_fallback=\"redact\" explicitly to use the full version"
    )


SQL_BODIES = {
    "redact_string": "CASE WHEN value IS NULL THEN NULL ELSE '[REDACTED]' END",
    "null_string": "CAST(NULL AS STRING)",
    "null_date": "CAST(NULL AS DATE)",
    "null_timestamp": "CAST(NULL AS TIMESTAMP)",
    "null_numeric": "CAST(NULL AS DECIMAL(38, 9))",
    "last4_string": "CASE WHEN value IS NULL THEN NULL WHEN length(regexp_replace(trim(value), '[\\\\s-]+', '')) <= 4 THEN '[REDACTED]' ELSE concat(repeat('*', length(regexp_replace(trim(value), '[\\\\s-]+', '')) - 4), right(regexp_replace(trim(value), '[\\\\s-]+', ''), 4)) END",
    "email_partial_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) NOT RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$' THEN '[REDACTED]' ELSE concat(left(trim(value), 1), '***@', lower(substring_index(trim(value), '@', -1))) END",
    "initials_string": "CASE WHEN value IS NULL THEN NULL WHEN size(filter(split(trim(value), '[^\\\\p{L}]+'), x -> x <> '')) <= 1 THEN '[REDACTED]' ELSE aggregate(filter(split(trim(value), '[^\\\\p{L}]+'), x -> x <> ''), '', (a, x) -> concat(a, upper(left(x, 1)))) END",
    "year_date": "CASE WHEN value IS NULL THEN NULL ELSE make_date(year(value), 1, 1) END",
    "year_timestamp": "CASE WHEN value IS NULL THEN NULL ELSE make_timestamp(year(value), 1, 1, 0, 0, 0, 'UTC') END",
    "age_band_10_numeric": "CASE WHEN value IS NULL OR isnan(CAST(value AS DOUBLE)) OR abs(CAST(value AS DOUBLE)) = CAST('Infinity' AS DOUBLE) OR value < 0 THEN NULL ELSE CAST(floor(value / 10) * 10 AS BIGINT) END",
    "credit_score_band_50_numeric": "CASE WHEN value IS NULL OR isnan(CAST(value AS DOUBLE)) OR abs(CAST(value AS DOUBLE)) = CAST('Infinity' AS DOUBLE) OR value < 0 THEN NULL ELSE CAST(floor(value / 50) * 50 AS BIGINT) END",
    "rounded_numeric": "CASE WHEN value IS NULL OR isnan(CAST(value AS DOUBLE)) OR abs(CAST(value AS DOUBLE)) = CAST('Infinity' AS DOUBLE) THEN NULL ELSE round(value, -3) END",
    "location_1dp_numeric": "CASE WHEN value IS NULL OR isnan(CAST(value AS DOUBLE)) OR abs(CAST(value AS DOUBLE)) = CAST('Infinity' AS DOUBLE) THEN NULL ELSE round(value, 1) END",
    "ip_network_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) RLIKE '^(?:25[0-5]|2[0-4]\\\\d|1?\\\\d?\\\\d)(?:\\\\.(?:25[0-5]|2[0-4]\\\\d|1?\\\\d?\\\\d)){3}$' THEN concat(regexp_extract(trim(value), '^(\\\\d+\\\\.\\\\d+\\\\.\\\\d+)\\\\.', 1), '.0/24') WHEN lower(trim(value)) RLIKE '^[0-9a-f:]+$' AND trim(value) LIKE '%:%' AND regexp_extract(lower(trim(value)), '^((?:[0-9a-f]{1,4}:){4})', 1) <> '' THEN concat(regexp_extract(lower(trim(value)), '^((?:[0-9a-f]{1,4}:){4})', 1), ':/64') ELSE '[REDACTED]' END",
    "mac_vendor_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) RLIKE '^(?i)[0-9a-f]{2}([:-][0-9a-f]{2}){5}$' THEN concat(upper(regexp_replace(substring(trim(value), 1, 8), '-', ':')), ':**:**:**') ELSE '[REDACTED]' END",
    "url_domain_string": "CASE WHEN value IS NULL THEN NULL WHEN try_parse_url(trim(value), 'PROTOCOL') IN ('http', 'https') AND try_parse_url(trim(value), 'HOST') IS NOT NULL THEN lower(try_parse_url(trim(value), 'HOST')) ELSE '[REDACTED]' END",
    "prefix_3_string": "CASE WHEN value IS NULL THEN NULL WHEN length(trim(value)) <= 3 THEN '[REDACTED]' ELSE concat(left(trim(value), 3), '***') END",
    "raw_string": "value", "raw_date": "value", "raw_timestamp": "value", "raw_numeric": "value"
}

# Expand IPv6 to eight hextets in SQL, select the first four, and canonicalize
# each with base conversion.  This covers compressed and fully expanded forms
# without depending on session state or a user-defined function.
SQL_BODIES["ip_network_string"] = """CASE
WHEN value IS NULL THEN NULL
WHEN trim(value) RLIKE '^(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)(?:\\.(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)){3}$'
  THEN concat(regexp_extract(trim(value), '^(\\d+\\.\\d+\\.\\d+)\\.', 1), '.0/24')
WHEN lower(trim(value)) RLIKE '^[0-9a-f:]+$' AND trim(value) LIKE '%:%'
  THEN concat(concat_ws(':', transform(slice(
    concat(
      filter(split(split(lower(trim(value)), '::', 2)[0], ':'), x -> x <> ''),
      array_repeat('0', 8
        - size(filter(split(split(lower(trim(value)), '::', 2)[0], ':'), x -> x <> ''))
        - CASE WHEN instr(lower(trim(value)), '::') > 0
            THEN size(filter(split(split(lower(trim(value)), '::', 2)[1], ':'), x -> x <> '')) ELSE 0 END),
      CASE WHEN instr(lower(trim(value)), '::') > 0
        THEN filter(split(split(lower(trim(value)), '::', 2)[1], ':'), x -> x <> '') ELSE array() END),
    1, 4), x -> lower(conv(conv(x, 16, 10), 10, 16)))), '::/64')
ELSE '[REDACTED]' END"""


def sql_body(version: str, sql_type: str) -> str:
    """Return a mask expression whose result has exactly ``sql_type``."""
    family = type_family(sql_type)
    if version == "hmac_sha256":
        if family == "STRING":
            return f"CAST((gr_hmac_sha256(value)) AS {sql_type})"
        if family == "NUMERIC":
            return f"CAST((gr_hmac_sha256_numeric(value)) AS {sql_type})"
        return f"CAST(NULL AS {sql_type})"
    if version in {"redacted", "null"} and family != "STRING":
        return f"CAST(NULL AS {sql_type})"
    body_name = load_library()["versions"].get(version, {}).get(family)
    if not body_name or body_name not in SQL_BODIES:
        return ("CASE WHEN value IS NULL THEN CAST(NULL AS STRING) ELSE '[REDACTED]' END"
                if family == "STRING" else f"CAST(NULL AS {sql_type})")
    return f"CAST(({SQL_BODIES[body_name]}) AS {sql_type})"


HMAC_PYTHON_BODY = '''import hashlib\nimport hmac\nimport re\nimport unicodedata\nfrom databricks.secrets import get\n_key = get(catalog="{{CATALOG}}", schema="{{SCHEMA}}", key="hmac_key").encode("utf-8")\ndef _digest(normalized):\n    return hmac.new(_key, normalized.encode("utf-8"), hashlib.sha256).digest()\ndef h(value):\n    if value is None:\n        return None\n    normalized = re.sub(r"[\\s-]+", "", unicodedata.normalize("NFKC", str(value).strip()).upper())\n    return _digest(normalized).hex()\ndef h_numeric(value):\n    if value is None or not value.is_finite():\n        return None\n    if value == 0:\n        normalized = "0"\n    else:\n        normalized = format(value.normalize(), "f")\n        if "." in normalized:\n            normalized = normalized.rstrip("0").rstrip(".")\n    return int.from_bytes(_digest(normalized)[:8], "big") & ((1 << 63) - 1)\n'''


def hmac_function_ddl(catalog: str, schema: str) -> str:
    body = HMAC_PYTHON_BODY.replace("{{CATALOG}}", catalog).replace("{{SCHEMA}}", schema)
    return f"""CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`gr_hmac_sha256_python`(value STRING)
RETURNS STRING
LANGUAGE PYTHON
HANDLER 'h'
SECRETS (`{catalog}`.`{schema}`.`hmac_key`)
ENVIRONMENT (environment_version = '6')
AS $${body}$$;
CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`gr_hmac_sha256`(value STRING)
RETURNS STRING
RETURN `{catalog}`.`{schema}`.`gr_hmac_sha256_python`(value);
CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric_python`(value DECIMAL(38, 18))
RETURNS BIGINT
LANGUAGE PYTHON
HANDLER 'h_numeric'
SECRETS (`{catalog}`.`{schema}`.`hmac_key`)
ENVIRONMENT (environment_version = '6')
AS $${body}$$;
CREATE OR REPLACE FUNCTION `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric`(value DECIMAL(38, 18))
RETURNS BIGINT
RETURN `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric_python`(value);"""
