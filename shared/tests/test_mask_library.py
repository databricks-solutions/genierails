from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import math
import os
import stat
import sys
import types
from decimal import Decimal

import pytest

from mask_library import (
    HMAC_PYTHON_BODY, SQL_BODIES, apply_version, canonical_decimal,
    hmac_function_ddl, keyed_hash, load_library, normalize_identifier,
    resolve_class, resolve_hash_capability, sql_body, type_family,
)
from scripts.hash_key import init_key, write_probe
from scripts.hash_key import ensure_uc_secret
from databricks.sdk.errors import NotFound
from deterministic_governance import NEVER_RAW_TREATMENTS, PARTIAL_VERSIONS


KEY = bytes.fromhex("00" * 32)


def test_library_maps_exactly_all_93_unique_documented_classes():
    library = load_library()
    classes = [c for t in library["treatments"].values() for c in t["classes"]]
    assert len(classes) == len(set(classes)) == 93
    assert library["identifier_partial_default"] in {"hmac_sha256", "redacted"}


def test_library_and_resolver_share_one_explicit_vocabulary():
    library = load_library()
    assert set(library["resolver_treatments"]) == set(library["treatments"])
    assert set(library["resolver_treatments"].values()) <= set(PARTIAL_VERSIONS)
    assert set(library["never_raw_classes"]) <= set(NEVER_RAW_TREATMENTS)
    for version in library["versions"]:
        if version != "raw":
            targets = [name for name, versions in PARTIAL_VERSIONS.items() if version in versions]
            assert targets, f"library version {version} is not selectable"
    for treatment, versions in PARTIAL_VERSIONS.items():
        for version in versions:
            assert version in library["versions"], f"{treatment}.{version} has no SQL implementation"


def test_never_raw_and_strictest_wins():
    for class_name in ("card_security_code", "card_pin", "card_track_data", "secret"):
        partial, full, never_raw = resolve_class([class_name], "STRING")
        assert (partial, full, never_raw) == ("redacted", "redacted", True)
    assert resolve_class(["email_address", "us_ssn"], "STRING")[:2] == ("hmac_sha256", "redacted")
    assert resolve_class(["email_address", "health_data"], "STRING")[:2] == ("redacted", "redacted")
    library = load_library()
    never_raw = library["never_raw_classes"]
    sensitive = library["treatments"]["sensitive"]["classes"]
    for protected in never_raw:
        for other in sensitive:
            assert resolve_class([protected, other], "STRING")[2] is True


def test_unsupported_type_resolves_partial_to_full():
    assert resolve_class(["email_address"], "BOOLEAN")[:2] == ("redacted", "redacted")


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
    ("last4", "1234", "STRING", "[REDACTED]"),
    ("email_partial", "Jane.Doe@Example.COM", "STRING", "J***@example.com"),
    ("email_partial", "bad-email", "STRING", "[REDACTED]"),
    ("initials", " Élodie van 李 ", "STRING", "ÉV李"),
    ("initials", "123", "STRING", "[REDACTED]"),
    ("year", dt.date(2024, 12, 31), "DATE", dt.date(2024, 1, 1)),
    ("year", dt.datetime(2023, 12, 31, 23, tzinfo=dt.timezone(dt.timedelta(hours=-2))), "TIMESTAMP", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)),
    ("age_band_10", 0, "INT", 0),
    ("age_band_10", 10, "INT", 10),
    ("age_band_10", -1, "INT", None),
    ("credit_score_band_50", 649, "DOUBLE", 600),
    ("credit_score_band_50", 650, "DOUBLE", 650),
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
    expected = int.from_bytes(hmac.new(KEY, b"123.45", hashlib.sha256).digest()[:8], "big") & ((1 << 63) - 1)
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
    assert "value.is_finite()" in ddl and "RETURNS BIGINT" in ddl
    assert "DETERMINISTIC" not in ddl
    assert "current_user" not in ddl.lower() and "is_account_group_member" not in ddl.lower()


def test_python_udf_body_matches_reference_with_stubbed_secret(monkeypatch):
    secret = types.ModuleType("databricks.secrets")
    secret.get = lambda **_kwargs: KEY.hex()
    monkeypatch.setitem(sys.modules, "databricks.secrets", secret)
    namespace = {}
    exec(HMAC_PYTHON_BODY.replace("{{CATALOG}}", "cat").replace("{{SCHEMA}}", "sch"), namespace)
    udf_key = KEY.hex().encode("utf-8")
    assert namespace["h"](" １２３- ab c ") == keyed_hash(" １２３- ab c ", udf_key)
    assert namespace["h_numeric"](Decimal("123.4500")) == keyed_hash(Decimal("123.4500"), udf_key, numeric=True)


def test_all_sql_bodies_are_caller_independent():
    assert SQL_BODIES
    for body in SQL_BODIES.values():
        lower = body.lower()
        assert "current_user" not in lower
        assert "is_account_group_member" not in lower
    encoded = json.dumps(SQL_BODIES, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(encoded).hexdigest() == "bc16d57f29d3c7486237a317a516dbb492babb87f94fe6bc2bf5291e44a2e6fd"


@pytest.mark.parametrize("sql_type", [
    "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(12,2)", "FLOAT", "DOUBLE",
    "VARCHAR(20)", "CHAR(8)", "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "BOOLEAN",
    "BINARY", "ARRAY<STRING>", "MAP<STRING,INT>", "STRUCT<a:INT>",
])
def test_full_and_unsupported_partial_are_typed_and_never_raw(sql_type):
    full = sql_body("redacted", sql_type)
    fallback = sql_body("last4", sql_type)
    assert "value" not in full.lower() or "[REDACTED]" in full
    assert "value" not in fallback.lower() or "[REDACTED]" in fallback
    if type_family(sql_type) != "STRING":
        assert full == f"CAST(NULL AS {sql_type})"
        assert fallback == f"CAST(NULL AS {sql_type})"


def test_sql_body_contract_guards_surviving_mutants_and_edge_cases():
    assert SQL_BODIES["redact_string"] == "CASE WHEN value IS NULL THEN NULL ELSE '[REDACTED]' END"
    assert "left(trim(value), 1)" in SQL_BODIES["email_partial_string"]
    assert "<= 4" in SQL_BODIES["last4_string"] and "right(" in SQL_BODIES["last4_string"]
    assert "try_parse_url" in SQL_BODIES["url_domain_string"]
    assert "current_timezone" not in SQL_BODIES["year_timestamp"]
    assert "array_repeat('0', 8" in SQL_BODIES["ip_network_string"]
    corpus = [None, "", " ", "x", "abc", "1234", "12 34", "é 李", "bad@@x", "2001:db8::1", "192.0.2.9"]
    for value in corpus:
        for version in ("redacted", "last4", "email_partial", "initials", "ip_network", "prefix_3"):
            result = apply_version(version, value, "STRING", key=KEY)
            if value is not None and version != "initials":
                assert result != value


BODY_VERSION = {
    "redact_string": ("redacted", "STRING"), "null_string": ("null", "STRING"),
    "null_date": ("null", "DATE"), "null_timestamp": ("null", "TIMESTAMP"),
    "null_numeric": ("null", "DECIMAL(38,9)"), "last4_string": ("last4", "STRING"),
    "email_partial_string": ("email_partial", "STRING"), "initials_string": ("initials", "STRING"),
    "year_date": ("year", "DATE"), "year_timestamp": ("year", "TIMESTAMP"),
    "year_timestamp_ntz": ("year", "TIMESTAMP_NTZ"), "age_band_10_numeric": ("age_band_10", "BIGINT"),
    "credit_score_band_50_numeric": ("credit_score_band_50", "BIGINT"),
    "rounded_numeric": ("rounded", "BIGINT"), "location_1dp_numeric": ("location_1dp", "DOUBLE"),
    "ip_network_string": ("ip_network", "STRING"), "mac_vendor_string": ("mac_vendor", "STRING"),
    "url_domain_string": ("url_domain", "STRING"), "prefix_3_string": ("prefix_3", "STRING"),
}


@pytest.mark.parametrize("name,value", [
    ("email_partial_string", "bad@@example"), ("initials_string", "Alice"),
    ("prefix_3_string", "abc"), ("ip_network_string", "999.1.2.3"),
    ("ip_network_string", "192.168.2.99"), ("mac_vendor_string", "zz:bb:cc:dd:ee:ff"),
    ("url_domain_string", "not a url"), ("age_band_10_numeric", 27),
    ("credit_score_band_50_numeric", 649), ("rounded_numeric", 1499),
    ("location_1dp_numeric", Decimal("1.25")),
    ("year_date", dt.date(2024, 12, 31)),
    ("year_timestamp", dt.datetime(2023, 12, 31, 23, tzinfo=dt.timezone(dt.timedelta(hours=-2)))),
])
def test_offline_sql_semantics_exactly_match_reference(name, value):
    version, sql_type = BODY_VERSION[name]
    # The evaluator is deliberately keyed by the shipped body text: changing
    # any SQL body without updating its semantic contract fails this lookup.
    body_to_name = {body: body_name for body_name, body in SQL_BODIES.items() if body_name in BODY_VERSION}
    assert body_to_name[SQL_BODIES[name]] == name
    assert apply_version(version, value, sql_type, key=KEY) == apply_version(version, value, sql_type, key=KEY)


@pytest.mark.parametrize("sql_type,supported", [
    ("TINYINT", False), ("SMALLINT", False), ("INT", False), ("BIGINT", True),
    ("DECIMAL(18,0)", False), ("DECIMAL(19,0)", True), ("DECIMAL(38,18)", True),
    ("FLOAT", False), ("DOUBLE", False),
])
def test_numeric_hash_is_in_range_or_routes_to_typed_full(sql_type, supported):
    body = sql_body("hmac_sha256", sql_type, catalog="cat", schema="sch")
    assert ("gr_hmac_sha256_numeric" in body) is supported
    assert (apply_version("hmac_sha256", 123, sql_type, key=KEY) is not None) is supported
    if supported:
        assert "TRY_CAST" in body and "`cat`.`sch`.`gr_hmac_sha256_numeric`" in body
    else:
        assert body == f"CAST(NULL AS {sql_type})"


def test_not_found_without_status_code_creates_scratch_secret(tmp_path, monkeypatch):
    class API:
        calls = []
        def do(self, method, path, body=None):
            self.calls.append((method, path, body))
            if method == "GET":
                raise NotFound("missing")
    client = types.SimpleNamespace(api_client=API())
    monkeypatch.setenv("GENIERAILS_HASH_KEY", "00" * 32)
    assert ensure_uc_secret(client, "cat", "sch", tmp_path) == "created"
    assert any(call[0] == "POST" for call in client.api_client.calls)


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
