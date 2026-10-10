"""Deterministic, caller-independent mask-library reference implementation."""

from __future__ import annotations

import datetime as dt
import ipaddress
import json
import re
from decimal import Context, Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from pathlib import Path
from urllib.parse import urlsplit

from deterministic_governance import NEVER_RAW_TREATMENTS, AccessResolution, resolve_precedence

LIBRARY_PATH = Path(__file__).with_name("mask_library.json")
REDACTED = "[REDACTED]"
NUMERIC_TYPES = frozenset({"BYTE", "TINYINT", "SHORT", "SMALLINT", "INT", "INTEGER", "LONG", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"})


def load_library(path: Path = LIBRARY_PATH) -> dict:
    return json.loads(path.read_text())


def type_family(sql_type: str) -> str:
    value = sql_type.upper().split("(", 1)[0].strip()
    if value in {"CHAR", "VARCHAR"}:
        return "STRING"
    return "NUMERIC" if value in NUMERIC_TYPES else value


BIGINT_MAX = 2**63 - 1
# Wide enough for DECIMAL(38, x) arithmetic without the default 28-digit rounding.
_WIDE = Context(prec=80, rounding=ROUND_HALF_UP)
INTEGRAL_RANGES = {
    "BYTE": 2**7, "TINYINT": 2**7, "SHORT": 2**15, "SMALLINT": 2**15,
    "INT": 2**31, "INTEGER": 2**31, "LONG": 2**63, "BIGINT": 2**63,
}


def _decimal_38_18(value: object) -> Decimal | None:
    """Mirror SQL try_cast(value AS DECIMAL(38, 18)): NULL for NaN, infinity,
    and anything with 20 or more integer digits; otherwise round half up."""
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite() or number.copy_abs() >= 10**20:
        return None
    with localcontext(_WIDE):
        number = number.quantize(Decimal("1E-18"))
        return number if number.copy_abs() < 10**20 else None


def _try_cast_numeric(value: Decimal | int | None, sql_type: str) -> object:
    """Mirror the outer TRY_CAST(... AS sql_type) of a numeric mask."""
    if value is None:
        return None
    upper = sql_type.upper().replace(" ", "")
    if upper == "NUMERIC":
        return value  # the type family itself: a raw body with no outer cast
    base = upper.split("(", 1)[0]
    if base in INTEGRAL_RANGES:
        whole = int(value)  # truncates toward zero, like TRY_CAST
        return whole if -INTEGRAL_RANGES[base] <= whole < INTEGRAL_RANGES[base] else None
    if base in {"DECIMAL", "NUMERIC"}:
        match = re.fullmatch(r"[A-Z]+\((\d+)(?:,(\d+))?\)", upper)
        precision, scale = (int(match.group(1)), int(match.group(2) or 0)) if match else (10, 0)
        with localcontext(_WIDE):
            fitted = Decimal(value).quantize(Decimal(1).scaleb(-scale))
            return fitted if fitted.copy_abs() < 10 ** (precision - scale) else None
    if base in {"DOUBLE", "FLOAT"}:
        return float(value)
    return value


def apply_version(version: str, value: object, sql_type: str) -> object:
    """Apply a named library version. Malformed/unsupported inputs get its full value."""
    if value is None:
        return None
    family = type_family(sql_type)
    if version == "raw":
        return _try_cast_numeric(value, sql_type) if family == "NUMERIC" else value
    if version in {"redacted", "null"}:
        return REDACTED if version == "redacted" and family == "STRING" else None
    if version == "last4" and family == "STRING":
        text = str(value).strip()
        compact = re.sub(r"[\s-]+", "", text)
        return "*" * (len(compact) - 4) + compact[-4:] if len(compact) > 4 else REDACTED
    if version == "partial" and family == "STRING":
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
        if family == "TIMESTAMP_NTZ" and isinstance(value, dt.datetime) and value.tzinfo is None:
            return dt.datetime(value.year, 1, 1)
        if family == "TIMESTAMP" and isinstance(value, dt.datetime) and value.tzinfo is not None:
            return dt.datetime(value.astimezone(dt.timezone.utc).year, 1, 1, tzinfo=dt.timezone.utc)
        return None
    if version in {"age_band_10", "credit_score_band_50"} and family == "NUMERIC":
        number = _decimal_38_18(value)
        if number is None or number < 0:
            return None
        width = 10 if version == "age_band_10" else 50
        with localcontext(_WIDE):
            lower = int(number // width) * width
        return _try_cast_numeric(lower if lower <= BIGINT_MAX else None, sql_type)
    if version in {"rounded", "location_1dp"} and family == "NUMERIC":
        number = _decimal_38_18(value)
        if number is None:
            return None
        quantum = Decimal("1E3") if version == "rounded" else Decimal("0.1")
        with localcontext(_WIDE):
            return _try_cast_numeric(number.quantize(quantum), sql_type)
    if version == "ip_network" and family == "STRING":
        # IPv4 keeps its /24 network; IPv6 and anything else is redacted.
        try:
            address = ipaddress.IPv4Address(str(value).strip())
        except ValueError:
            return REDACTED
        return str(ipaddress.IPv4Network(f"{address}/24", strict=False))
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


def resolve_class(classes: list[str], sql_type: str, library: dict | None = None) -> tuple[str, str, str, bool]:
    """Resolve several class tags to (treatment, partial, full, never_raw).

    G17 strictest-wins: the strongest partial version wins; equal strength is
    broken by the greatest treatment name so tag order never matters.
    """
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
            rank = order["redacted"] if partial in {"redacted", "null"} else order["partial"]
            candidates.append((rank, treatment_name, partial, treatment["full"]))
    if not candidates:
        raise KeyError(f"no mask-library mapping for {classes!r}")
    never_raw = any(candidate[1] in NEVER_RAW_TREATMENTS for candidate in candidates)
    _, treatment_name, partial, full = max(candidates)
    return treatment_name, partial, full, never_raw


def resolve_column_access(classes: list[str], sql_type: str, *, library: dict | None = None, **precedence) -> AccessResolution:
    """Resolve a column's class tags, then access, carrying the never-raw flag."""
    treatment, partial, _full, never_raw = resolve_class(classes, sql_type, library)
    return resolve_precedence(treatment=treatment, library_default=partial, never_raw=never_raw, **precedence)


# Every body is non-throwing under ANSI mode, so a mask can never fail a query:
# numeric bodies go through try_cast(value AS DECIMAL(38, 18)), which is NULL
# for NaN, infinity and values of 1E20 or more, and then use only try_* steps
# or operations that cannot overflow DECIMAL(38, 18). sql_body() adds TRY_CAST.
SQL_BODIES = {
    "redact_string": "CASE WHEN value IS NULL THEN NULL ELSE '[REDACTED]' END",
    "null_string": "CAST(NULL AS STRING)",
    "null_date": "CAST(NULL AS DATE)",
    "null_timestamp": "CAST(NULL AS TIMESTAMP)",
    "null_timestamp_ntz": "CAST(NULL AS TIMESTAMP_NTZ)",
    "null_numeric": "CAST(NULL AS DECIMAL(38, 9))",
    "last4_string": "CASE WHEN value IS NULL THEN NULL WHEN length(regexp_replace(trim(value), '[\\\\s-]+', '')) <= 4 THEN '[REDACTED]' ELSE concat(repeat('*', length(regexp_replace(trim(value), '[\\\\s-]+', '')) - 4), right(regexp_replace(trim(value), '[\\\\s-]+', ''), 4)) END",
    "email_partial_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) NOT RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$' THEN '[REDACTED]' ELSE concat(left(trim(value), 1), '***@', lower(substring_index(trim(value), '@', -1))) END",
    "initials_string": "CASE WHEN value IS NULL THEN NULL WHEN size(filter(split(trim(value), '[^\\\\p{L}]+'), x -> x <> '')) <= 1 THEN '[REDACTED]' ELSE aggregate(filter(split(trim(value), '[^\\\\p{L}]+'), x -> x <> ''), '', (a, x) -> concat(a, upper(left(x, 1)))) END",
    "year_date": "CASE WHEN value IS NULL THEN NULL ELSE make_date(year(value), 1, 1) END",
    "year_timestamp": "CASE WHEN value IS NULL THEN NULL ELSE make_timestamp(year(convert_timezone('UTC', value)), 1, 1, 0, 0, 0, 'UTC') END",
    "year_timestamp_ntz": "CASE WHEN value IS NULL THEN NULL ELSE make_timestamp_ntz(year(value), 1, 1, 0, 0, 0) END",
    "age_band_10_numeric": "CASE WHEN try_cast(value AS DECIMAL(38, 18)) IS NULL OR try_cast(value AS DECIMAL(38, 18)) < 0 THEN NULL ELSE try_cast(floor(try_divide(try_cast(value AS DECIMAL(38, 18)), 10)) * 10 AS BIGINT) END",
    "credit_score_band_50_numeric": "CASE WHEN try_cast(value AS DECIMAL(38, 18)) IS NULL OR try_cast(value AS DECIMAL(38, 18)) < 0 THEN NULL ELSE try_cast(floor(try_divide(try_cast(value AS DECIMAL(38, 18)), 50)) * 50 AS BIGINT) END",
    "rounded_numeric": "CASE WHEN try_cast(value AS DECIMAL(38, 18)) IS NULL THEN NULL ELSE round(try_cast(value AS DECIMAL(38, 18)), -3) END",
    "location_1dp_numeric": "CASE WHEN try_cast(value AS DECIMAL(38, 18)) IS NULL THEN NULL ELSE round(try_cast(value AS DECIMAL(38, 18)), 1) END",
    "ip_network_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) RLIKE '^(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?:[.](?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])){3}$' THEN concat(regexp_extract(trim(value), '^([0-9]+[.][0-9]+[.][0-9]+)[.]', 1), '.0/24') ELSE '[REDACTED]' END",
    "mac_vendor_string": "CASE WHEN value IS NULL THEN NULL WHEN trim(value) RLIKE '^(?i)[0-9a-f]{2}([:-][0-9a-f]{2}){5}$' THEN concat(upper(regexp_replace(substring(trim(value), 1, 8), '-', ':')), ':**:**:**') ELSE '[REDACTED]' END",
    "url_domain_string": "CASE WHEN value IS NULL THEN NULL WHEN try_parse_url(trim(value), 'PROTOCOL') IN ('http', 'https') AND try_parse_url(trim(value), 'HOST') IS NOT NULL THEN lower(try_parse_url(trim(value), 'HOST')) ELSE '[REDACTED]' END",
    "prefix_3_string": "CASE WHEN value IS NULL THEN NULL WHEN length(trim(value)) <= 3 THEN '[REDACTED]' ELSE concat(left(trim(value), 3), '***') END",
    "raw_string": "value", "raw_date": "value", "raw_timestamp": "value", "raw_numeric": "value"
}


def sql_body(version: str, sql_type: str) -> str:
    """Return a mask expression whose result has exactly ``sql_type``."""
    family = type_family(sql_type)
    if version in {"redacted", "null"} and family != "STRING":
        return f"CAST(NULL AS {sql_type})"
    body_name = load_library()["versions"].get(version, {}).get(family)
    if not body_name or body_name not in SQL_BODIES:
        return ("CASE WHEN value IS NULL THEN CAST(NULL AS STRING) ELSE '[REDACTED]' END"
                if family == "STRING" else f"CAST(NULL AS {sql_type})")
    cast = "TRY_CAST" if family == "NUMERIC" else "CAST"
    return f"{cast}(({SQL_BODIES[body_name]}) AS {sql_type})"
