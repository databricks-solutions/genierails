from __future__ import annotations

import datetime as dt
import json
import types
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from mask_library import SQL_BODIES, apply_version, load_library, resolve_class, resolve_column_access, sql_body, type_family
from scripts.live_mask_library import BODY_CASES, RENDER_ZONE, TYPED_CASES, comparable
from deterministic_governance import NEVER_RAW_TREATMENTS, PARTIAL_VERSIONS, AccessResolution, validate_config



def test_library_maps_exactly_all_93_unique_documented_classes():
    library = load_library()
    classes = [c for t in library["treatments"].values() for c in t["classes"]]
    assert len(classes) == len(set(classes)) == 93


def test_library_uses_the_shared_treatment_vocabulary_directly():
    library = load_library()
    for name, treatment in library["treatments"].items():
        assert name in PARTIAL_VERSIONS, f"{name} is not configurable via treatment_versions"
        partials = set(treatment.get("partial_by_type", {}).values()) | {treatment.get("partial")} - {None}
        assert partials <= PARTIAL_VERSIONS[name], name
        assert treatment["full"] in {"redacted", "null"}
    for treatment, versions in PARTIAL_VERSIONS.items():
        for version in versions:
            assert version in library["versions"], f"{treatment}.{version} has no SQL implementation"


def _config_errors(**overrides):
    return validate_config({"access_tier_groups": ["t1", "t2", "t3"], **overrides})


@pytest.mark.parametrize("treatment", ["identifier", "age", "credit_score", "ip_address", "mac_address", "url", "location"])
def test_each_former_generic_treatment_is_configurable_on_its_own(treatment):
    assert _config_errors(treatment_versions={treatment: {"partial": "redacted"}}) == []
    assert resolve_class([f"class.{treatment}" if treatment != "identifier" else "class.passport"], "STRING")[0] == treatment


def test_identifiers_are_redacted_and_keyed_hash_is_refused():
    library = load_library()
    assert "hmac_sha256" not in library["versions"] and "hmac_sha256" not in library["protection_order"]
    for name in ("identifier", "ssn", "tfn_partial", "medicare_partial", "aadhaar_partial"):
        assert (library["treatments"][name]["partial"], library["treatments"][name]["full"]) == ("redacted", "redacted")
    assert resolve_class(["passport"], "STRING")[:3] == ("identifier", "redacted", "redacted")
    assert resolve_class(["us_ssn"], "BIGINT")[:3] == ("ssn", "redacted", "redacted")
    assert all("hmac_sha256" not in versions for versions in PARTIAL_VERSIONS.values())
    assert _config_errors(treatment_versions={"identifier": {"partial": "redacted"}}) == []
    assert _config_errors(treatment_versions={"ssn": {"partial": "last4"}}) == []
    for treatment in ("identifier", "ssn", "email_partial"):
        errors = _config_errors(treatment_versions={treatment: {"partial": "hmac_sha256"}})
        assert errors == [f'treatment_versions["{treatment}"].partial: keyed hash is not available in this version; use redacted']
    errors = _config_errors(column_overrides={"c.s.t.x": {"partial": "hmac_sha256"}})
    assert errors == ['column_overrides["c.s.t.x"].partial: keyed hash is not available in this version; use redacted']


def test_every_never_raw_class_lands_on_a_never_raw_treatment():
    library = load_library()
    owner = {c: name for name, t in library["treatments"].items() for c in t["classes"]}
    assert set(library["never_raw_classes"]) == {"card_security_code", "card_pin", "card_track_data", "secret"}
    for class_name in library["never_raw_classes"]:
        assert owner[class_name] in NEVER_RAW_TREATMENTS, f"{class_name} resolves to {owner[class_name]}"
        for sql_type in ("STRING", "DATE", "BIGINT"):
            treatment, partial, full, never_raw = resolve_class([class_name], sql_type)
            assert treatment in NEVER_RAW_TREATMENTS and never_raw is True
            assert (partial, full) == ("redacted", "redacted")
    # Only the never-raw classes may land there.
    for name in NEVER_RAW_TREATMENTS:
        assert set(library["treatments"][name]["classes"]) <= set(library["never_raw_classes"])


PRINCIPALS = {
    "tier1": dict(group="t1", principal="analyst"),
    "exempt": dict(group="outsider", principal="etl"),
    "deployer": dict(group="outsider", principal="deployer"),
    "tier1_deployer": dict(group="t1", principal="deployer"),
}


def test_never_raw_classes_are_never_raw_for_anyone_alone_or_combined():
    library = load_library()
    classes = [c for t in library["treatments"].values() for c in t["classes"]]
    access = dict(column="c.s.t.x", access_tier_groups=["t1", "t2", "t3"],
                  deployer_principal="deployer", raw_exempt_principals=["etl"], library=library)
    for protected in library["never_raw_classes"]:
        for tags in [[protected]] + [pair for other in classes if other != protected
                                      for pair in ([protected, other], [other, protected])]:
            for who, principal in PRINCIPALS.items():
                result = resolve_column_access(tags, "STRING", **access, **principal)
                assert result == AccessResolution("full"), (tags, who, result)
    # Ordinary classes still give tier 1, exempt principals and the deployer raw.
    for who, principal in PRINCIPALS.items():
        assert resolve_column_access(["email_address"], "STRING", **access, **principal) == AccessResolution("raw"), who


def test_strictest_wins():
    assert resolve_class(["email_address", "us_ssn"], "STRING")[:3] == ("ssn", "redacted", "redacted")
    assert resolve_class(["email_address", "health_data"], "STRING")[:3] == ("redact", "redacted", "redacted")
    library = load_library()
    for protected in library["never_raw_classes"]:
        for other in library["treatments"]["redact"]["classes"]:
            assert resolve_class([protected, other], "STRING")[3] is True
            assert resolve_class([other, protected], "STRING")[3] is True


def test_strictest_wins_tie_break_is_order_independent():
    # email (partial) and phone (last4) rank equally; the greater treatment name wins.
    assert resolve_class(["email_address", "phone_number"], "STRING") == ("phone_partial", "last4", "redacted", False)
    assert resolve_class(["phone_number", "email_address"], "STRING") == ("phone_partial", "last4", "redacted", False)
    # Two equally strong redacted treatments: still deterministic.
    assert resolve_class(["us_ssn", "passport"], "STRING")[0] == resolve_class(["passport", "us_ssn"], "STRING")[0] == "ssn"
    # A redacted never-raw treatment beats an equally strong ordinary one either way.
    assert resolve_class(["health_data", "card_pin"], "STRING")[0] == resolve_class(["card_pin", "health_data"], "STRING")[0] == "redact"
    assert resolve_class(["health_data", "card_pin"], "STRING")[3] is True


def test_unsupported_type_resolves_partial_to_full():
    assert resolve_class(["email_address"], "BOOLEAN")[1:3] == ("redacted", "redacted")
    assert resolve_class(["date_of_birth"], "TIMESTAMP_NTZ")[1:3] == ("year", "null")


@pytest.mark.parametrize("version, value, sql_type, expected", [
    ("redacted", "secret", "STRING", "[REDACTED]"),
    ("redacted", dt.date(2020, 2, 3), "DATE", None),
    ("null", "secret", "STRING", None),
    ("last4", "4111-1111-1111-1234", "STRING", "************1234"),
    ("last4", "1234", "STRING", "[REDACTED]"),
    ("partial", "Jane.Doe@Example.COM", "STRING", "J***@example.com"),
    ("partial", "bad-email", "STRING", "[REDACTED]"),
    ("initials", " Élodie van 李 ", "STRING", "ÉV李"),
    ("initials", "123", "STRING", "[REDACTED]"),
    ("year", dt.date(2024, 12, 31), "DATE", dt.date(2024, 1, 1)),
    ("year", dt.datetime(2023, 12, 31, 23, tzinfo=dt.timezone(dt.timedelta(hours=-2))), "TIMESTAMP", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)),
    ("year", dt.datetime(2024, 12, 31, 23, 30), "TIMESTAMP_NTZ", dt.datetime(2024, 1, 1)),
    ("year", dt.datetime(2024, 12, 31, 23, 30), "TIMESTAMP", None),
    ("age_band_10", 0, "INT", 0),
    ("age_band_10", 10, "INT", 10),
    ("age_band_10", -1, "INT", None),
    ("credit_score_band_50", 649, "DOUBLE", 600),
    ("credit_score_band_50", 650, "DOUBLE", 650),
    ("rounded", Decimal("1499"), "DECIMAL(10,2)", Decimal("1E+3")),
    ("rounded", Decimal("1500"), "DECIMAL(10,2)", Decimal("2E+3")),
    ("rounded", Decimal("-1500"), "DECIMAL(10,2)", Decimal("-2E+3")),
    ("location_1dp", Decimal("1.25"), "DOUBLE", 1.3),
    ("location_1dp", Decimal("-1.25"), "DOUBLE", -1.3),
    ("ip_network", "192.168.2.99", "STRING", "192.168.2.0/24"),
    ("ip_network", "2001:db8:abcd:12:1234::1", "STRING", "[REDACTED]"),
    ("ip_network", "192.168.2.09", "STRING", "[REDACTED]"),
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
    assert apply_version(version, value, sql_type) == expected


@pytest.mark.parametrize("version,sql_type", [
    ("redacted", "STRING"), ("null", "DATE"), ("last4", "STRING"),
    ("partial", "STRING"), ("initials", "STRING"), ("year", "DATE"), ("year", "TIMESTAMP_NTZ"),
    ("age_band_10", "INT"), ("credit_score_band_50", "DOUBLE"),
    ("rounded", "DECIMAL"), ("location_1dp", "DOUBLE"),
    ("ip_network", "STRING"), ("mac_vendor", "STRING"),
    ("url_domain", "STRING"), ("prefix_3", "STRING"),
    ("raw", "STRING"),
])
def test_every_version_preserves_null(version, sql_type):
    assert apply_version(version, None, sql_type) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("version", ["rounded", "location_1dp", "age_band_10", "credit_score_band_50"])
def test_non_finite_numbers_are_null(version, bad):
    assert apply_version(version, bad, "DOUBLE") is None


def test_all_sql_bodies_are_caller_independent():
    assert SQL_BODIES
    for body in SQL_BODIES.values():
        lower = body.lower()
        assert "current_user" not in lower
        assert "is_account_group_member" not in lower


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


def _body_versions():
    """Shipped body name -> (version, type family) from the library table."""
    pairs = {}
    for version, by_family in load_library()["versions"].items():
        for family, body in by_family.items():
            # A bare family name means a raw body with no outer cast.
            pairs.setdefault(body, (version, family))
    return pairs


def _rendered(value):
    """The Python value as to_json(..., TO_JSON_OPTIONS) renders it."""
    if isinstance(value, dt.datetime) and value.tzinfo is None:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dt.datetime):
        local = value.astimezone(ZoneInfo(RENDER_ZONE))
        offset = local.strftime("%z")
        return local.strftime("%Y-%m-%dT%H:%M:%S.") + f"{local.microsecond // 1000:03d}" + offset[:3] + ":" + offset[3:]
    if isinstance(value, dt.date):
        return value.isoformat()
    return comparable(value)


def test_every_shipped_sql_body_has_exact_expected_cases():
    assert set(BODY_CASES) == set(SQL_BODIES)
    numeric = {"age_band_10", "credit_score_band_50", "rounded", "location_1dp"}
    assert {version for version, *_ in TYPED_CASES} >= numeric
    assert set(SQL_BODIES) <= set(_body_versions())


# No offline Spark SQL engine exists, so the SQL bodies themselves run only in
# the live test (DATABRICKS_LIVE_TESTS=1), which asserts these same exact
# values. Offline, the Python reference must produce them exactly.
@pytest.mark.parametrize("body,python_input,expected", [
    (body, python_input, expected)
    for body, cases in BODY_CASES.items() for _sql, python_input, expected in cases
])
def test_reference_produces_exact_expected_value_per_body(body, python_input, expected):
    version, family = _body_versions()[body]
    assert _rendered(apply_version(version, python_input, family)) == comparable(expected)


@pytest.mark.parametrize("version,sql_type,python_input,expected", [
    (version, sql_type, python_input, expected) for version, sql_type, _sql, python_input, expected in TYPED_CASES
])
def test_reference_produces_exact_expected_value_per_typed_mask(version, sql_type, python_input, expected):
    assert _rendered(apply_version(version, python_input, sql_type)) == comparable(expected)


NON_THROWING = ("try_cast(", "try_divide(", "round(", "floor(", "CASE WHEN", "CAST(NULL AS")


def test_numeric_masks_use_only_non_throwing_operations():
    # A plain CAST, division or multiplication of the column can raise an
    # overflow error under ANSI mode; every numeric step goes through try_cast.
    for name, body in SQL_BODIES.items():
        if name.endswith("_numeric") and body != "value":
            # The column is only ever read through the non-throwing try_cast.
            assert body.startswith("CAST(NULL AS") or "try_cast(value AS DECIMAL(38, 18))" in body, name
            assert "value" not in body.replace("try_cast(value AS DECIMAL(38, 18))", ""), name
    for sql_type in ("TINYINT", "INT", "BIGINT", "DECIMAL(38,0)", "DECIMAL(38,10)", "DOUBLE"):
        for version in ("age_band_10", "credit_score_band_50", "rounded", "location_1dp", "raw"):
            assert sql_body(version, sql_type).startswith("TRY_CAST((")


class _FakeWarehouse:
    """Answers the live harness's statements with each case's expected value."""

    def __init__(self, wrong_body=None):
        self.wrong_body, self.statements = wrong_body, []
        self.statement_execution = self

    def execute_statement(self, *, warehouse_id, statement, wait_timeout):
        self.statements.append(statement)
        rows = []
        checks = [(body, SQL_BODIES[body], sql_input, expected)
                  for body, cases in BODY_CASES.items() for sql_input, _python_input, expected in cases]
        checks += [(f"{version}:{sql_type}", sql_body(version, sql_type), sql_input, expected)
                   for version, sql_type, sql_input, _python_input, expected in TYPED_CASES]
        for name, expression, sql_input, expected in checks:
            if f"named_struct('v', {expression})," in statement and f"SELECT {sql_input} AS value" in statement:
                value = "tampered" if name == self.wrong_body else expected
                rendered = str(value) if isinstance(value, Decimal) else json.dumps(value)
                rows = [['{"v": ' + rendered + '}']]
        ok = types.SimpleNamespace(state="SUCCEEDED", error=None)
        return types.SimpleNamespace(status=ok, result=types.SimpleNamespace(data_array=rows))


@pytest.mark.parametrize("wrong_body", [None, "last4_string", "year_timestamp", "rounded:BIGINT"])
def test_live_harness_checks_exact_values_per_statement_zone(wrong_body):
    from scripts.live_mask_library import TO_JSON_OPTIONS, run
    warehouse = _FakeWarehouse(wrong_body)
    result = run(warehouse, "wh")
    assert result["bodies_tested"] == sum(len(cases) for cases in BODY_CASES.values()) + len(TYPED_CASES)
    assert {d["body"] for d in result["details"]} == ({wrong_body} if wrong_body else set())
    # Read-only: only the per-body SELECTs run, each rendered in a non-UTC zone.
    assert all(s.startswith("SELECT to_json(") for s in warehouse.statements)
    assert all(s.split(" FROM (SELECT ")[0].endswith(f", {TO_JSON_OPTIONS})") for s in warehouse.statements)
    assert f"'timeZone', '{RENDER_ZONE}'" in TO_JSON_OPTIONS and RENDER_ZONE != "UTC"
