from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import math
import os
import stat
from decimal import Decimal

import pytest

from mask_library import (
    HMAC_PYTHON_BODY, SQL_BODIES, apply_version, canonical_decimal,
    hmac_function_ddl, keyed_hash, load_library, normalize_identifier,
    resolve_class, resolve_hash_capability,
)
from scripts.hash_key import init_key, write_probe


KEY = bytes.fromhex("00" * 32)


def test_library_maps_exactly_all_93_unique_documented_classes():
    library = load_library()
    classes = [c for t in library["treatments"].values() for c in t["classes"]]
    assert len(classes) == len(set(classes)) == 93
    assert library["identifier_partial_default"] in {"hmac_sha256", "redacted"}


def test_never_raw_and_strictest_wins():
    for class_name in ("card_security_code", "card_pin", "card_track_data", "secret"):
        partial, full, never_raw = resolve_class([class_name], "STRING")
        assert (partial, full, never_raw) == ("redacted", "redacted", True)
    assert resolve_class(["email_address", "us_ssn"], "STRING")[:2] == ("hmac_sha256", "redacted")
    assert resolve_class(["email_address", "health_data"], "STRING")[:2] == ("redacted", "redacted")


def test_hash_capability_is_lazy_and_fails_closed():
    assert resolve_hash_capability("last4", "redacted", available=False, fallback=None) == "last4"
    assert resolve_hash_capability("hmac_sha256", "redacted", available=False, fallback="redact") == "redacted"
    assert resolve_hash_capability("hmac_sha256", "redacted", available=True, fallback=None) == "hmac_sha256"
    with pytest.raises(RuntimeError, match="hash_fallback"):
        resolve_hash_capability("hmac_sha256", "redacted", available=False, fallback=None)


@pytest.mark.parametrize("version, value, sql_type, expected", [
    ("redacted", "secret", "STRING", "[REDACTED]"),
    ("redacted", dt.date(2020, 2, 3), "DATE", None),
    ("null", "secret", "STRING", None),
    ("last4", "4111-1111-1111-1234", "STRING", "************1234"),
    ("last4", "12", "STRING", "[REDACTED]"),
    ("email_partial", "Jane.Doe@Example.COM", "STRING", "J***@example.com"),
    ("email_partial", "bad-email", "STRING", "[REDACTED]"),
    ("initials", " Élodie van 李 ", "STRING", "ÉV李"),
    ("initials", "123", "STRING", "[REDACTED]"),
    ("year", dt.date(2024, 12, 31), "DATE", dt.date(2024, 1, 1)),
    ("year", dt.datetime(2023, 12, 31, 23, tzinfo=dt.timezone(dt.timedelta(hours=-2))), "TIMESTAMP", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)),
    ("age_band_10", 0, "INT", "0-9"),
    ("age_band_10", 10, "INT", "10-19"),
    ("age_band_10", -1, "INT", None),
    ("credit_score_band_50", 649, "DOUBLE", "600-649"),
    ("credit_score_band_50", 650, "DOUBLE", "650-699"),
    ("rounded", Decimal("1499"), "DECIMAL(10,2)", Decimal("1E+3")),
    ("rounded", Decimal("1500"), "DECIMAL(10,2)", Decimal("2E+3")),
    ("rounded", Decimal("-1500"), "DECIMAL(10,2)", Decimal("-2E+3")),
    ("location_1dp", Decimal("1.25"), "DOUBLE", Decimal("1.3")),
    ("location_1dp", Decimal("-1.25"), "DOUBLE", Decimal("-1.3")),
    ("ip_network", "192.168.2.99", "STRING", "192.168.2.0/24"),
    ("ip_network", "2001:db8:abcd:12:1234::1", "STRING", "2001:db8:abcd:12::/64"),
    ("ip_network", "not-an-ip", "STRING", "[REDACTED]"),
    ("mac_vendor", "aa-bb-cc-dd-ee-ff", "STRING", "AA:BB:CC:**:**:**"),
    ("mac_vendor", "broken", "STRING", "[REDACTED]"),
    ("url_domain", "https://User:pass@EXAMPLE.com:443/a?q=1", "STRING", "example.com"),
    ("url_domain", "javascript:alert(1)", "STRING", "[REDACTED]"),
    ("prefix_3", "2000", "STRING", "200***"),
    ("prefix_3", "x", "STRING", "[REDACTED]"),
    ("raw", "x", "STRING", "x"),
])
def test_exact_version_outputs(version, value, sql_type, expected):
    assert apply_version(version, value, sql_type, key=KEY) == expected


@pytest.mark.parametrize("version,sql_type", [
    ("redacted", "STRING"), ("null", "DATE"), ("last4", "STRING"),
    ("email_partial", "STRING"), ("initials", "STRING"), ("year", "DATE"),
    ("age_band_10", "INT"), ("credit_score_band_50", "DOUBLE"),
    ("rounded", "DECIMAL"), ("location_1dp", "DOUBLE"),
    ("ip_network", "STRING"), ("mac_vendor", "STRING"),
    ("url_domain", "STRING"), ("prefix_3", "STRING"),
    ("hmac_sha256", "STRING"), ("raw", "STRING"),
])
def test_every_version_preserves_null(version, sql_type):
    assert apply_version(version, None, sql_type, key=KEY) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("version", ["rounded", "location_1dp", "age_band_10", "credit_score_band_50"])
def test_non_finite_numbers_are_null(version, bad):
    assert apply_version(version, bad, "DOUBLE") is None


def test_identifier_normalization_unicode_and_numeric_canonical_form():
    assert normalize_identifier(" １２３- ab c ") == "123ABC"
    assert canonical_decimal("00123.4500") == "123.45"
    assert canonical_decimal("-0.00") == "0"
    expected = hmac.new(KEY, b"123.45", hashlib.sha256).hexdigest()
    assert keyed_hash(Decimal("00123.4500"), KEY, numeric=True) == expected
    assert apply_version("hmac_sha256", Decimal("123.450"), "DECIMAL", key=KEY) == expected


def test_hash_ddl_uses_module_scope_secret_named_handler_and_is_not_deterministic():
    ddl = hmac_function_ddl("cat", "sch")
    assert "LANGUAGE PYTHON" in ddl and "HANDLER 'h'" in ddl
    assert "SECRETS (`cat`.`sch`.`hmac_key`)" in ddl
    assert "environment_version = '6'" in ddl
    assert '_key = get(catalog="cat", schema="sch", key="hmac_key")' in ddl
    assert ddl.index("_key = get") < ddl.index("def h")
    assert "HANDLER 'h_numeric'" in ddl
    assert "value DECIMAL(38, 18)" in ddl
    assert "value.is_finite()" in ddl
    assert "DETERMINISTIC" not in ddl
    assert "current_user" not in ddl.lower() and "is_account_group_member" not in ddl.lower()


def test_all_sql_bodies_are_caller_independent():
    assert SQL_BODIES
    for body in SQL_BODIES.values():
        lower = body.lower()
        assert "current_user" not in lower
        assert "is_account_group_member" not in lower


def test_init_key_is_32_bytes_0600_and_never_prints_value(tmp_path, capsys):
    path = tmp_path / "key"
    init_key(path)
    value = path.read_text().strip()
    assert len(bytes.fromhex(value)) == 32
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert value not in capsys.readouterr().out


def test_probe_shape(tmp_path):
    digest = "a" * 64
    path = tmp_path / "generated" / "hash_probe.json"
    write_probe(path, digest)
    assert json.loads(path.read_text()) == {"input": "genierails-probe", "hmac_sha256": digest}
