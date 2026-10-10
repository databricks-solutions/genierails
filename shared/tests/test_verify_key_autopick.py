"""verify-access picks a provably safe row-pairing key per masked table.

Users never choose VERIFY_KEY_COLUMN: each masked table gets an explicit
verify_key_columns entry, else the global key when the table has it, else its
single-column PRIMARY KEY, else an id-like column — untagged, unmasked, of an
exactly-pairing type — that the admin proves unique and non-null. An explicit
key is validated the same way and never silently replaced. The live layer runs
against a fake multi-table SQL warehouse (information_schema included), the
make targets against real make with a stub sub-make.
"""
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import hcl2
import pytest

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED.parent
sys.path.insert(0, str(SHARED))
sys.path.insert(0, str(SHARED / "scripts"))

import verify_effective_access as vea  # noqa: E402
from verify_effective_access import (  # noqa: E402
    DEFAULT_ADMIN_TIER,
    INCONCLUSIVE,
    PASS,
    SOURCE_GLOBAL_KEY,
    SOURCE_ID_LIKE,
    SOURCE_PRIMARY_KEY,
    SOURCE_TABLE_KEY,
    ColumnMaskCheck,
    TableKeyFacts,
    TestPrincipal as VerificationPrincipal,
    VerificationSpec,
    key_candidates,
    main,
    no_key_message,
    pick_table_key,
)
import release_helpers as rh  # noqa: E402
import remap_env_config  # noqa: E402
import resolve_env_config  # noqa: E402
import saved_settings  # noqa: E402

SALT = "autopick-test-salt"
ANALYSTS = "analysts"   # masked tier
OPS = "ops"             # unmasked tier


def _masked(_value):
    return "XXX-MASKED"


@dataclass
class Table:
    columns: list                       # [(name, data type)]
    rows: list                          # [{column: value}]
    pk: list = field(default_factory=list)
    tags: dict = field(default_factory=dict)      # live column tags: column -> [(name, value)]
    col_masks: set = field(default_factory=set)   # columns with a direct column mask


class FakeWarehouse:
    """Several tables, per-tier column masks, and the information_schema views
    the key picker reads. Rows tying on the ORDER BY key come back in a
    different order for the admin than for every other principal."""

    def __init__(self, tables, *, masks=None, broken_metadata=False):
        self.tables = tables                # "cat.sch.tbl" -> Table
        self.masks = masks or {}            # tier -> {"cat.sch.tbl.col": fn}
        self.broken_metadata = broken_metadata
        self.statements = []                # (tier, sql, params)

    def view(self, tier, table):
        t = self.tables[table]
        masks = self.masks.get(tier, {})
        out = [{c: masks[f"{table}.{c}"](v) if f"{table}.{c}" in masks else v for c, v in r.items()}
               for r in t.rows]
        return out if tier == DEFAULT_ADMIN_TIER else out[::-1]

    @staticmethod
    def _keep(where_col, in_list, params):
        if where_col is None:
            return lambda r: True
        wanted = {params[name.strip().lstrip(":")] for name in in_list.split(",")}
        return lambda r: r[where_col] is not None and str(r[where_col]) in wanted

    def run(self, tier, sql, params):
        params = dict(params or {})
        self.statements.append((tier, sql, params))
        sql = sql.replace("`", "")      # identifiers are backtick-quoted; names here are plain
        cell = lambda v: None if v is None else str(v)  # noqa: E731 — data_array is strings
        m = re.fullmatch(r"SELECT (COUNT\(\*\)|tag_name, tag_value) FROM "
                         r"system\.information_schema\.(column_masks|column_tags) .*", sql)
        if m:
            if self.broken_metadata:
                raise RuntimeError("Query failed (FAILED): [TABLE_OR_VIEW_NOT_FOUND] system.information_schema")
            table = self.tables.get(f"{params['c']}.{params['s']}.{params['t']}")
            if m.group(2) == "column_tags":
                tags = {c.lower(): t for c, t in (table.tags if table else {}).items()}.get(params["k"], [])
                return [list(t) for t in tags] if m.group(1) != "COUNT(*)" else [[str(len(tags))]]
            return [[str(int(table is not None and params["k"] in {c.lower() for c in table.col_masks}))]]
        if "information_schema" in sql:
            if self.broken_metadata:
                raise RuntimeError("Query failed (FAILED): [TABLE_OR_VIEW_NOT_FOUND] system.information_schema")
            table = self.tables.get(f"{params['c']}.{params['s']}.{params['t']}")
            if table is None:
                return []
            if "information_schema.columns" in sql:
                return [[name, data_type] for name, data_type in table.columns]
            if "table_constraints" in sql:
                return [[c] for c in table.pk]
            if "SELECT column_name, tag_name, tag_value FROM system.information_schema.column_tags" in sql:
                return [[c, n, v] for c, tags in table.tags.items() for n, v in tags]
        m = re.fullmatch(
            r"SELECT (\w+), (\w+) FROM (\S+)(?: WHERE (\w+) IN \(([^)]*)\))? "
            r"ORDER BY (\w+|\w+ IS NULL DESC, xxhash64\(:salt, \w+\)) LIMIT (\d+)", sql)
        if m:
            key, col, table, where_col, in_list, order, limit = m.groups()
            rows = [r for r in self.view(tier, table) if self._keep(where_col, in_list, params)(r)]
            if "xxhash64" in order:
                # NULLs first, then a salted hash of the key; repeats stay adjacent.
                rows.sort(key=lambda r: (r[key] is not None, hashlib.sha256(
                    f"{params['salt']}|{r[key]}".encode()).hexdigest()))
            else:
                rows.sort(key=lambda r: (r[key] is not None, str(r[key])))
            return [[cell(r[key]), cell(r[col])] for r in rows[: int(limit)]]
        m = re.fullmatch(
            r"SELECT COUNT\(\*\), COUNT\(DISTINCT (\w+)\), COUNT\(\w+\) FROM (\S+) "
            r"WHERE (\w+) IN \(([^)]*)\)", sql)
        if m:
            key, table, where_col, in_list = m.groups()
            rows = [r for r in self.view(tier, table) if self._keep(where_col, in_list, params)(r)]
            vals = [r[key] for r in rows]
            return [[str(len(rows)), str(len({v for v in vals if v is not None})),
                     str(sum(v is not None for v in vals))]]
        m = re.fullmatch(r"SELECT COUNT\(\*\) FROM (\S+)(?: WHERE (\w+) IN \(([^)]*)\))?", sql)
        if m:
            table, where_col, in_list = m.groups()
            return [[str(sum(1 for r in self.view(tier, table) if self._keep(where_col, in_list, params)(r)))]]
        raise AssertionError(f"fake warehouse cannot run: {sql}")


@pytest.fixture
def warehouse(monkeypatch):
    holder = {}

    class FakeVerifier(vea.EffectiveAccessVerifier):
        def resolve_principal_groups(self, memberships):
            return list(memberships)

        def provision_principal(self, tier, memberships=None, *, resolved_groups=None):
            return VerificationPrincipal(tier, f"verify-{tier}", f"app-{tier}", "secret", f"sp-{tier}")

        def grant_warehouse_use(self, principal):
            holder.setdefault("grants", []).append(principal.tier)

        def deprovision_principal(self, principal):
            pass

        def _ws_for(self, principal):
            return principal.tier

        def run_query(self, ws, sql, parameters=None):
            return holder["wh"].run(ws, sql.strip(), parameters)

    monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
    monkeypatch.setenv("GENIERAILS_VERIFY_PROPAGATION_SLEEP", "0")
    monkeypatch.setenv(vea.SAMPLE_SALT_ENV, SALT)
    monkeypatch.setattr(vea, "load_auth", lambda path: {
        "host": "h", "client_id": "admin-app", "client_secret": "s"})
    monkeypatch.setattr(vea, "EffectiveAccessVerifier", FakeVerifier)

    def install(wh):
        holder["wh"] = wh
        holder["grants"] = []
        wh.grants = holder["grants"]
        return wh
    return install


# ── fixtures: a sample-style schema (customers / payments / notes) ──────────

def _ids(prefix, n=30):
    return [f"{prefix}-{i:04d}" for i in range(1, n + 1)]


def sample_tables(n=30):
    """Like shared/examples/dev_to_prod's sample: customer_id unique on each table."""
    cust = _ids("C", n)
    return {
        "cat.sch.customers": Table(
            [("customer_id", "STRING"), ("full_name", "STRING"), ("ssn", "STRING")],
            [{"customer_id": c, "full_name": f"NAME-{c}", "ssn": f"SSN-{i:04d}-RAW"} for i, c in enumerate(cust)],
            tags={"ssn": [("class.us_ssn", None)], "full_name": [("class.name", None)]}),
        "cat.sch.payments": Table(
            [("payment_id", "STRING"), ("customer_id", "STRING"), ("credit_card_number", "STRING"),
             ("amount", "DECIMAL(12,2)")],
            [{"payment_id": p, "customer_id": c, "credit_card_number": f"CARD-{i:04d}-RAW", "amount": "1.00"}
             for i, (p, c) in enumerate(zip(_ids("P", n), cust))],
            tags={"credit_card_number": [("class.credit_card", None)]}),
        "cat.sch.notes": Table(
            [("note_id", "STRING"), ("customer_id", "STRING"), ("free_text", "STRING")],
            [{"note_id": nid, "customer_id": c, "free_text": f"NOTE-{i:04d}-RAW"}
             for i, (nid, c) in enumerate(zip(_ids("N", n), cust))],
            tags={"free_text": [("class.free_text", None)]}),
    }


SAMPLE_MASKED = {"cat.sch.customers": "ssn", "cat.sch.payments": "credit_card_number", "cat.sch.notes": "free_text"}


def _analyst_masks(masked=SAMPLE_MASKED):
    return {ANALYSTS: {f"{t}.{c}": _masked for t, c in masked.items()}}


def _abac(tmp_path, masked=SAMPLE_MASKED, extra_tags=()):
    tags = [(f"{t}.{c}", "gr_treatment", "redact") for t, c in masked.items()] + list(extra_tags)
    tfvars = tmp_path / "abac.auto.tfvars"
    tfvars.write_text(
        'fgac_policies = [\n  { name = "mask_redact", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",\n'
        f'    to_principals = ["{ANALYSTS}"], match_condition = "hasTagValue(\'gr_treatment\', \'redact\')" }},\n]\n'
        "tag_assignments = [\n"
        + "".join(f'  {{ entity_type = "columns", entity_name = "{e}", tag_key = "{k}", tag_value = "{v}" }},\n'
                  for e, k, v in tags)
        + "]\n")
    account = tmp_path / "account.auto.tfvars"
    account.write_text(f'groups = {{ {ANALYSTS} = {{}}, {OPS} = {{}} }}\n')
    return tfvars, account


def _env_file(tmp_path, text=""):
    path = tmp_path / "env.auto.tfvars"
    path.write_text(text)
    return path


def _main(tmp_path, *extra, env_text="", masked=SAMPLE_MASKED, extra_tags=()):
    tfvars, account = _abac(tmp_path, masked, extra_tags)
    return main(["--from-tfvars", str(tfvars), "--account-tfvars", str(account),
                 "--env-file", str(_env_file(tmp_path, env_text)),
                 "--auth-file", str(tmp_path / "auth.auto.tfvars"), "--warehouse-id", "wh-1", *extra])


def _values(tables):
    """Every row value (keys included) of the fake tables — none may be printed."""
    return {str(v) for t in tables.values() for r in t.rows for v in r.values() if v is not None}


def _assert_no_values(text, tables):
    leaked = sorted(v for v in _values(tables) if v in text and len(v) > 3)
    assert not leaked, f"row/key values printed: {leaked[:5]}"


# ── 1. ranking (pure) ───────────────────────────────────────────────────────

def test_single_column_primary_key_comes_first():
    facts = TableKeyFacts((("order_ref", "STRING"), ("order_id", "BIGINT"), ("id", "INT")), ("order_ref",))
    assert key_candidates("c.s.orders", facts)[0] == ("order_ref", SOURCE_PRIMARY_KEY)


def test_id_like_tie_break_table_singular_then_id_then_other_ids_by_type_then_name():
    facts = TableKeyFacts((
        ("zone_id", "STRING"), ("account_id", "DOUBLE"), ("branch_id", "SMALLINT"), ("id", "STRING"),
        ("customer_id", "BIGINT"), ("agent_id", "INT"), ("category_id", "STRING"), ("name", "STRING"),
    ))
    assert key_candidates("c.s.categories", facts) == [
        ("category_id", SOURCE_ID_LIKE),   # <table singular>_id
        ("id", SOURCE_ID_LIKE),
        ("agent_id", SOURCE_ID_LIKE),      # preferred types (string/int/long), alphabetical
        ("customer_id", SOURCE_ID_LIKE),
        ("zone_id", SOURCE_ID_LIKE),
        ("branch_id", SOURCE_ID_LIKE),     # acceptable type last; DOUBLE never pairs exactly
    ]


@pytest.mark.parametrize("table, expected", [
    ("c.s.customers", "customer_id"), ("c.s.branches", "branch_id"), ("c.s.categories", "category_id"),
    ("c.s.boxes", "box_id"), ("c.s.customer", "customer_id"),
])
def test_singular_table_names(table, expected):
    facts = TableKeyFacts(((expected, "STRING"), ("a_id", "STRING")))
    assert key_candidates(table, facts)[0][0] == expected


def test_composite_tagged_masked_or_unsafe_type_primary_keys_are_not_picked():
    cols = (("pk", "STRING"), ("ssn", "STRING"), ("ts", "TIMESTAMP"), ("x_id", "STRING"))
    assert key_candidates("c.s.t", TableKeyFacts(cols, ("pk", "x_id")))[0] == ("x_id", SOURCE_ID_LIKE)
    assert key_candidates("c.s.t", TableKeyFacts(cols, ("pk",)), unsafe={"PK"})[0][0] == "x_id"
    assert key_candidates("c.s.t", TableKeyFacts(cols, ("ts",)))[0][0] == "x_id"


def test_only_plain_identifier_columns_are_auto_picked():
    facts = TableKeyFacts((("x` FROM t --_id", "STRING"), ("dash-ok_id", "STRING"), ("ok_id", "STRING")),
                          ("x` FROM t --_id",))
    assert key_candidates("c.s.t", facts) == [("dash-ok_id", SOURCE_ID_LIKE), ("ok_id", SOURCE_ID_LIKE)]


def test_an_unreadable_env_file_is_a_clean_error(tmp_path):
    tfvars, account = _abac(tmp_path)
    env = _env_file(tmp_path, "verify_key_columns = {\n")
    with pytest.raises(SystemExit, match="cannot read verify_key_columns from"):
        main(["--from-tfvars", str(tfvars), "--account-tfvars", str(account), "--env-file", str(env)])


def test_masked_id_columns_are_skipped_but_tags_are_left_to_the_key_checks():
    facts = TableKeyFacts((("t_id", "STRING"), ("id", "STRING"), ("o_id", "STRING")))
    assert [c for c, _ in key_candidates("c.s.t", facts, unsafe={"id"})] == ["t_id", "o_id"]


def test_unsafe_columns_come_from_the_shared_mask_matcher(tmp_path):
    tfvars, _ = _abac(tmp_path, extra_tags=[("cat.sch.payments.customer_id", "gr_treatment", "redact"),
                                            ("cat.sch.notes.note_id", "class", "identifier")])
    spec = vea.load_spec_from_tfvars(tfvars)
    unsafe = vea.unsafe_key_columns(spec.column_masks, spec.mask_config)
    assert "customer_id" in unsafe["cat.sch.payments"]       # a mask policy matches its tag
    assert "note_id" not in unsafe["cat.sch.notes"]          # class.* only: no mask matches it


def test_nothing_id_like_is_a_clear_per_table_refusal():
    pick = pick_table_key("c.s.t", facts=TableKeyFacts((("name", "STRING"),)), prove=lambda c: "")
    assert pick.key == "" and pick.problem == no_key_message("c.s.t")
    assert pick.problem == ('no provable row-pairing key for c.s.t; set verify_key_columns["c.s.t"] '
                            "or VERIFY_KEY_COLUMN")


def test_a_failed_auto_candidate_falls_through_but_an_explicit_one_does_not():
    facts = TableKeyFacts((("t_id", "STRING"), ("id", "STRING")))
    proofs = {"t_id": "not unique", "id": ""}
    auto = pick_table_key("c.s.t", facts=facts, prove=proofs.get)
    assert (auto.key, auto.source) == ("id", SOURCE_ID_LIKE)
    explicit = pick_table_key("c.s.t", explicit="t_id", facts=facts, prove=proofs.get)
    assert (explicit.key, explicit.source, explicit.problem) == ("", SOURCE_TABLE_KEY, "not unique")
    glob = pick_table_key("c.s.t", global_key="t_id", facts=facts, prove=proofs.get)
    assert (glob.key, glob.source, glob.problem) == ("", SOURCE_GLOBAL_KEY, "not unique")


def test_the_global_key_applies_only_where_the_table_has_it():
    facts = TableKeyFacts((("t_id", "STRING"),))
    pick = pick_table_key("c.s.t", global_key="customer_id", facts=facts, prove=lambda c: "")
    assert (pick.key, pick.source) == ("t_id", SOURCE_ID_LIKE)


def test_an_explicit_key_that_is_masked_or_missing_is_reported():
    facts = TableKeyFacts((("t_id", "STRING"), ("ssn", "STRING")))
    masked = pick_table_key("c.s.t", explicit="SSN", facts=facts, unsafe={"ssn"}, prove=lambda c: "")
    assert masked.problem == "row-pairing key SSN (verify_key_columns) is a masked column on c.s.t"
    missing = pick_table_key("c.s.t", global_key="t_id", explicit="nope", facts=facts, prove=lambda c: "")
    assert missing.problem == "row-pairing key nope (verify_key_columns) does not exist on c.s.t"


# ── 2. live picking (admin only) through --check-keys-only ──────────────────

def test_check_keys_only_picks_a_key_per_table_on_the_sample_schema(warehouse, tmp_path, capsys):
    wh = warehouse(FakeWarehouse(sample_tables()))
    assert _main(tmp_path, "--check-keys-only", "--require-mask-checks") == 0
    out = capsys.readouterr().out
    assert "Row-pairing key for cat.sch.customers: customer_id (id-like column)" in out
    assert "Row-pairing key for cat.sch.payments: payment_id (id-like column)" in out
    assert "Row-pairing key for cat.sch.notes: note_id (id-like column)" in out
    assert "All 3 masked table(s) have a proven row-pairing key." in out
    # Admin only: no test principal, no warehouse grant.
    assert {tier for tier, _, _ in wh.statements} == {DEFAULT_ADMIN_TIER} and wh.grants == []
    _assert_no_values(out, wh.tables)


def test_primary_key_is_picked(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.payments"].pk = ["customer_id"]
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only") == 0
    assert "Row-pairing key for cat.sch.payments: customer_id (primary key)" in capsys.readouterr().out


def test_candidates_with_mask_relevant_tags_are_skipped_live_and_from_the_config(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.payments"].tags["payment_id"] = [("gr_treatment", "redact")]   # live, a mask matches it
    tables["cat.sch.notes"].pk = ["free_text"]                                     # the masked column itself
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only",
                 extra_tags=[("cat.sch.customers.customer_id", "gr_treatment", "redact")]) == 2
    captured = capsys.readouterr()
    # payment_id ranks first (<table singular>_id) but its live tag is one a mask matches.
    assert "Row-pairing key for cat.sch.payments: customer_id (id-like column)" in captured.out
    assert "Row-pairing key for cat.sch.notes: note_id (id-like column)" in captured.out
    assert no_key_message("cat.sch.customers") in captured.err   # its only id is masked in the config


def test_a_mask_relevant_live_tag_is_named_when_nothing_else_is_left(warehouse, tmp_path, capsys):
    tables = {"cat.sch.notes": Table([("note_id", "STRING"), ("free_text", "STRING")],
                                     [{"note_id": f"N-{i}", "free_text": f"T-{i}"} for i in range(30)],
                                     tags={"note_id": [("gr_treatment", "redact")]})}
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.notes": "free_text"}) == 2
    assert ("note_id: row-pairing key note_id may be masked (it has 1 column tag(s) a column-mask "
            "policy matches) on cat.sch.notes") in capsys.readouterr().err


def _people(pk, extra_cols=(), tags=None):
    cols = [("email", "STRING"), ("ssn", "STRING"), ("contact_ref", "STRING"), ("secret", "STRING"), *extra_cols]
    rows = [{c: f"{c.upper()}-{i:04d}" for c, _ in cols} for i in range(30)]
    return {"cat.sch.people": Table(cols, rows, pk=[pk], tags=tags or {})}


@pytest.mark.parametrize("pk, tags", [
    ("email", {}),                                                       # unique, unmasked, untagged PK
    ("ssn", {}),
    ("contact_ref", {"contact_ref": [("class.email_address", None)]}),   # PII by its live class.* tag
])
def test_a_sensitive_primary_key_is_never_auto_picked(warehouse, tmp_path, capsys, pk, tags):
    warehouse(FakeWarehouse(_people(pk, tags=tags)))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}) == 2   # nothing else: refuse
    captured = capsys.readouterr()
    assert no_key_message("cat.sch.people") in captured.err
    assert f"cat.sch.people: {pk} (" not in captured.out

    warehouse(FakeWarehouse(_people(pk, extra_cols=[("person_id", "STRING")], tags=tags)))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}) == 0   # falls through
    assert "Row-pairing key for cat.sch.people: person_id (id-like column)" in capsys.readouterr().out


def test_a_pii_class_tag_in_the_config_also_rules_a_candidate_out(warehouse, tmp_path, capsys):
    warehouse(FakeWarehouse(_people("contact_ref", extra_cols=[("person_id", "STRING")])))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"},
                 extra_tags=[("cat.sch.people.contact_ref", "class.phone_number", "")]) == 0
    assert "Row-pairing key for cat.sch.people: person_id (id-like column)" in capsys.readouterr().out


def test_sensitive_key_reason_uses_the_coverage_and_classification_rules():
    assert vea.sensitive_key_reason("email") == "its name looks sensitive (email)"
    assert vea.sensitive_key_reason("ref", [("class.us_ssn", "")]) == "it is classified sensitive (class.us_ssn)"
    assert vea.sensitive_key_reason("customer_id", [("class.customer_identifier", "")]) == ""
    assert vea.sensitive_key_reason("total_amount") == ""        # first exposure doesn't block on amounts


NEW_EXCLUDED_CLASSES = [
    "class.ip", "class.ip_address", "class.IP-Address", "class.mac_address",
    "class.national_id", "class.national_identifier", "class.passport", "class.us_passport",
    "class.uk_passport", "class.driver_license", "class.us_driver_license", "class.tax_id",
    "class.geolocation", "class.location", "class.lat_long", "class.date_of_birth",
    "class.health_data", "class.medical_record_number", "class.biometric_data", "class.biometric",
]


@pytest.mark.parametrize("tag", NEW_EXCLUDED_CLASSES)
def test_each_sensitive_class_rules_out_an_innocently_named_primary_key(warehouse, tmp_path, capsys, tag):
    tags = {"contact_ref": [(tag, None)]}
    warehouse(FakeWarehouse(_people("contact_ref", tags=tags)))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}) == 2   # nothing else
    assert no_key_message("cat.sch.people") in capsys.readouterr().err
    warehouse(FakeWarehouse(_people("contact_ref", extra_cols=[("person_id", "STRING")], tags=tags)))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}) == 0
    assert "Row-pairing key for cat.sch.people: person_id (id-like column)" in capsys.readouterr().out


@pytest.mark.parametrize("tag", ["class.customer_id", "class.customer_identifier", "class.account_id",
                                 "class.user_id", "class.identifier", "class.transaction_id"])
def test_identifier_type_classes_stay_allowed(warehouse, tmp_path, capsys, tag):
    warehouse(FakeWarehouse(_people("contact_ref", tags={"contact_ref": [(tag, None)]})))
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}) == 0
    assert "Row-pairing key for cat.sch.people: contact_ref (primary key)" in capsys.readouterr().out


def test_key_exclusion_does_not_change_governance_mapping():
    import sensitivity_source as ss
    assert ss._CLASS_TO_GOVERNED.keys() <= ss.KEY_EXCLUDED_CLASS_SEMANTICS   # a superset
    for semantic in ("ip_address", "national_id", "passport", "mac_address", "geolocation"):
        assert semantic not in ss._CLASS_TO_GOVERNED     # still unmapped for governance
        assert ss.sensitive_class_semantic(f"class.{semantic}", "") == semantic
    assert ss.sensitive_class_semantic("class", "ip_address") == "ip_address"   # value-carried semantic
    assert ss.sensitive_class_semantic("pii", "ip_address") is None             # not a class tag


def test_a_sensitive_override_is_used_with_a_warning(warehouse, tmp_path, capsys):
    warehouse(FakeWarehouse(_people("email")))
    env_text = 'verify_key_columns = { "cat.sch.people" = "email" }\n'
    assert _main(tmp_path, "--check-keys-only", masked={"cat.sch.people": "secret"}, env_text=env_text) == 0
    out = capsys.readouterr().out
    assert "Row-pairing key for cat.sch.people: email (verify_key_columns)" in out
    assert ("WARNING: row-pairing key email (verify_key_columns) on cat.sch.people is used as you set it, "
            "but its name looks sensitive (email); prefer a non-sensitive id") in out
    assert "EMAIL-00" not in out


def test_class_only_tags_do_not_disqualify_a_candidate(warehouse, tmp_path, capsys):
    tables = sample_tables()
    for t in tables.values():
        t.tags["customer_id"] = [("class.customer_identifier", None)]        # native classifier tags
    tables["cat.sch.payments"].tags["payment_id"] = [("class.transaction_id", "")]
    tables["cat.sch.payments"].pk = ["payment_id"]
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only") == 0
    out = capsys.readouterr().out
    assert "Row-pairing key for cat.sch.customers: customer_id (id-like column)" in out
    assert "Row-pairing key for cat.sch.payments: payment_id (primary key)" in out


def test_a_column_masked_directly_falls_through_to_the_next(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.payments"].col_masks.add("payment_id")      # ALTER TABLE ... SET MASK, not a tag
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only") == 0
    assert "Row-pairing key for cat.sch.payments: customer_id (id-like column)" in capsys.readouterr().out
    env_text = 'verify_key_columns = { "cat.sch.payments" = "payment_id" }\n'
    assert _main(tmp_path, "--check-keys-only", env_text=env_text) == 2   # an override is not replaced
    assert ("Row-pairing key for cat.sch.payments: NONE — row-pairing key payment_id may be masked "
            "(it has 1 column mask(s))") in capsys.readouterr().out


def test_a_non_unique_candidate_falls_through_to_the_next(warehouse, tmp_path, capsys):
    tables = sample_tables()
    for i, row in enumerate(tables["cat.sch.payments"].rows):
        row["payment_id"] = f"P-{i // 2:04d}"                   # every payment_id twice
    wh = warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only") == 0
    out = capsys.readouterr().out
    assert "Row-pairing key for cat.sch.payments: customer_id (id-like column)" in out
    _assert_no_values(out, wh.tables)


def _salted_order(keys):
    return sorted(keys, key=lambda k: hashlib.sha256(f"{SALT}|{k}".encode()).hexdigest())


def test_a_repeat_past_the_sample_is_caught_by_the_whole_table_proof(warehouse, tmp_path, capsys):
    tables = sample_tables(n=40)
    rows = tables["cat.sch.notes"].rows
    order = _salted_order([r["note_id"] for r in rows])
    last_sampled, first_unsampled = order[vea.SAMPLE_ROWS - 1], order[vea.SAMPLE_ROWS]
    dup = next(r for r in rows if r["note_id"] == first_unsampled)
    dup["note_id"] = last_sampled       # the copy sorts right after it: past LIMIT 25
    dup["customer_id"] = None           # and the next candidate has a NULL
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only") == 2
    err = capsys.readouterr().err
    assert no_key_message("cat.sch.notes") in err
    assert "note_id: row-pairing key note_id is not unique / has NULLs (25 sampled keys match 26 rows" in err
    assert "customer_id: row-pairing key customer_id has NULLs" in err


def test_nothing_provable_is_a_clear_per_table_refusal(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.notes"] = Table([("body", "STRING"), ("free_text", "STRING")],
                                    [{"body": "b", "free_text": "f"}],
                                    tags={"free_text": [("class.free_text", None)]})
    wh = warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only", "--require-mask-checks") == 2
    captured = capsys.readouterr()
    assert ('no provable row-pairing key for cat.sch.notes; set verify_key_columns["cat.sch.notes"] '
            "or VERIFY_KEY_COLUMN") in captured.err
    assert "1 masked table(s) have no provable row-pairing key" in captured.err
    assert "Row-pairing key for cat.sch.customers: customer_id" in captured.out  # others still picked
    _assert_no_values(captured.out + captured.err, wh.tables)


def test_explicit_per_table_override_wins_and_is_still_validated(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.payments"].pk = ["payment_id"]
    warehouse(FakeWarehouse(tables))
    env_text = 'verify_key_columns = { "cat.sch.payments" = "customer_id" }\n'
    assert _main(tmp_path, "--check-keys-only", env_text=env_text) == 0
    assert "Row-pairing key for cat.sch.payments: customer_id (verify_key_columns)" in capsys.readouterr().out

    for row in tables["cat.sch.payments"].rows:
        row["customer_id"] = "C-SAME"                           # the override no longer pairs rows
    assert _main(tmp_path, "--check-keys-only", env_text=env_text) == 2
    captured = capsys.readouterr()
    assert "Row-pairing key for cat.sch.payments: NONE — row-pairing key customer_id is not unique" in captured.out
    assert "payment_id" not in captured.out.split("cat.sch.payments:")[1].splitlines()[0]  # no fall-through
    _assert_no_values(captured.out + captured.err, tables)


def test_a_missing_explicit_key_is_reported_not_replaced(warehouse, tmp_path, capsys):
    warehouse(FakeWarehouse(sample_tables()))
    env_text = 'verify_key_columns = { "CAT.SCH.NOTES" = "no_such_col" }\n'
    assert _main(tmp_path, "--check-keys-only", env_text=env_text) == 2
    out = capsys.readouterr().out
    assert ("Row-pairing key for cat.sch.notes: NONE — row-pairing key no_such_col (verify_key_columns) "
            "does not exist on cat.sch.notes") in out


def test_global_key_is_used_where_present_and_auto_elsewhere(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.customers"].columns.append(("account_ref", "STRING"))
    for i, row in enumerate(tables["cat.sch.customers"].rows):
        row["account_ref"] = f"A-{i:04d}"
    warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--check-keys-only", "--key-column", "account_ref") == 0
    out = capsys.readouterr().out
    assert "Row-pairing key for cat.sch.customers: account_ref (VERIFY_KEY_COLUMN)" in out
    assert "Row-pairing key for cat.sch.payments: payment_id (id-like column)" in out


def test_without_a_configured_warehouse_the_admin_check_uses_a_workspace_one(warehouse, tmp_path, monkeypatch):
    wh = warehouse(FakeWarehouse(sample_tables()))
    listed = [SimpleNamespace(id="other", name="x", state="STOPPED"),
              SimpleNamespace(id="abac", name="ABAC Serverless Warehouse", state="RUNNING")]
    monkeypatch.setattr(vea.EffectiveAccessVerifier, "admin_ws", property(
        lambda self: SimpleNamespace(warehouses=SimpleNamespace(list=lambda: listed))))
    tfvars, account = _abac(tmp_path)
    assert main(["--from-tfvars", str(tfvars), "--account-tfvars", str(account),
                 "--auth-file", str(tmp_path / "a"), "--check-keys-only"]) == 0
    assert wh.statements


def test_the_full_verification_never_borrows_a_workspace_warehouse(monkeypatch, tmp_path):
    monkeypatch.setenv("GENIERAILS_LIVE_VERIFY", "1")
    verifier = vea.EffectiveAccessVerifier({"host": "h", "client_id": "c", "client_secret": "s"})
    with pytest.raises(RuntimeError, match="Arbitrary workspace warehouse selection is disabled"):
        verifier.resolve_warehouse()


# ── 3. full live verification with auto-picked keys ─────────────────────────

def test_release_with_no_key_passes_on_the_sample_schema(warehouse, tmp_path, capsys):
    wh = warehouse(FakeWarehouse(sample_tables(), masks=_analyst_masks()))
    result = tmp_path / "result.json"
    assert _main(tmp_path, "--live", "--require-mask-checks", "--result-file", str(result)) == 0
    out = capsys.readouterr().out
    assert "RESULT: ALL EFFECTIVE (3 passed / 3)" in out
    proof = json.loads(result.read_text())
    assert proof["passed"] is True
    assert proof["mask_keys_proven_by_table"] == {
        "cat.sch.customers": "customer_id", "cat.sch.notes": "note_id", "cat.sch.payments": "payment_id"}
    _assert_no_values(out, wh.tables)


def test_mixed_tables_with_different_keys_and_a_leak_still_fails(warehouse, tmp_path, capsys):
    masks = _analyst_masks({"cat.sch.customers": "ssn", "cat.sch.payments": "credit_card_number"})
    wh = warehouse(FakeWarehouse(sample_tables(), masks=masks))     # notes.free_text NOT masked: a leak
    result = tmp_path / "result.json"
    env_text = 'verify_key_columns = { "cat.sch.payments" = "customer_id" }\n'
    assert _main(tmp_path, "--live", "--require-mask-checks", "--result-file", str(result),
                 env_text=env_text) == 1
    captured = capsys.readouterr()
    assert "Row-pairing key for cat.sch.payments: customer_id (verify_key_columns)" in captured.out
    assert "✗ [FAIL] column-mask cat.sch.notes.free_text" in captured.out
    proof = json.loads(result.read_text())
    assert proof["passed"] is False
    # Only tables whose every mask check passed are recorded, by table.
    assert proof["mask_keys_proven_by_table"] == {"cat.sch.customers": "customer_id",
                                                  "cat.sch.payments": "customer_id"}
    _assert_no_values(captured.out + captured.err, wh.tables)


def test_an_unkeyable_table_fails_release_and_rehearse_and_never_writes_a_pass(warehouse, tmp_path, capsys):
    tables = sample_tables()
    tables["cat.sch.notes"] = Table([("body", "STRING"), ("free_text", "STRING")],
                                    [{"body": "b", "free_text": "f"}],
                                    tags={"free_text": [("class.free_text", None)]})
    wh = warehouse(FakeWarehouse(tables, masks=_analyst_masks()))
    assert _main(tmp_path, "--live", "--require-mask-checks") == 1
    out = capsys.readouterr().out
    assert "✗ [INCONCLUSIVE] column-mask cat.sch.notes.free_text" in out
    assert no_key_message("cat.sch.notes") in out
    result = tmp_path / "result.json"
    assert _main(tmp_path, "--live", "--result-file", str(result)) == 1   # rehearse fails too
    out = capsys.readouterr().out
    assert "✗ [INCONCLUSIVE] column-mask cat.sch.notes.free_text" in out
    assert no_key_message("cat.sch.notes") in out and "NOT VERIFIED" in out
    proof = json.loads(result.read_text())
    assert proof["passed"] is False and proof["mask_keys_complete"] is False
    assert "cat.sch.notes" in proof["masked_tables"] and "cat.sch.notes" not in proof["mask_keys_proven_by_table"]
    _assert_no_values(out, wh.tables)


def test_an_unkeyable_table_is_refused_before_any_principal_is_provisioned(warehouse, tmp_path, capsys):
    tables = {"cat.sch.notes": Table([("body", "STRING"), ("free_text", "STRING")],
                                     [{"body": "b", "free_text": "f"}])}
    wh = warehouse(FakeWarehouse(tables))
    assert _main(tmp_path, "--live", "--require-mask-checks", masked={"cat.sch.notes": "free_text"}) == 1
    assert wh.grants == [] and {t for t, _, _ in wh.statements} == {DEFAULT_ADMIN_TIER}


def test_a_failing_explicit_key_blocks_even_in_rehearse(warehouse, tmp_path, capsys):
    tables = sample_tables()
    for row in tables["cat.sch.payments"].rows:
        row["customer_id"] = "C-SAME"
    warehouse(FakeWarehouse(tables, masks=_analyst_masks()))
    assert _main(tmp_path, "--live", env_text='verify_key_columns = { "cat.sch.payments" = "customer_id" }\n') == 1
    out = capsys.readouterr().out
    assert "✗ [INCONCLUSIVE] column-mask cat.sch.payments.credit_card_number" in out
    assert "row-pairing key customer_id is not unique" in out


def test_legacy_single_key_still_works(warehouse, tmp_path, capsys):
    tables = sample_tables()
    wh = warehouse(FakeWarehouse(tables, masks=_analyst_masks()))
    result = tmp_path / "result.json"
    assert _main(tmp_path, "--live", "--require-mask-checks", "--key-column", "customer_id",
                 "--result-file", str(result), env_text='verify_key_column = "customer_id"\n') == 0
    out = capsys.readouterr().out
    assert out.count("customer_id (VERIFY_KEY_COLUMN)") == 3
    proof = json.loads(result.read_text())
    assert proof["mask_checks_passed_by_key"] == {"customer_id": 3}
    assert set(proof["mask_keys_proven_by_table"].values()) == {"customer_id"}

    # Without information_schema access the global key is still the one tried
    # everywhere (no auto-pick replaces it), and the unreadable masks/tags
    # check refuses it: fail-closed, and reported per table.
    wh.broken_metadata = True
    assert _main(tmp_path, "--live", "--require-mask-checks", "--key-column", "customer_id") == 1
    out = capsys.readouterr().out
    assert out.count("customer_id (VERIFY_KEY_COLUMN)") == 3
    assert out.count("could not read its column masks/tags as the admin baseline") == 3


def test_a_tagged_global_key_is_still_refused_up_front(tmp_path):
    with pytest.raises(SystemExit, match="row-pairing key ssn may be masked for .* on cat.sch.customers"):
        _main(tmp_path, "--key-column", "ssn")


def test_an_override_with_only_a_class_tag_in_the_config_is_accepted(warehouse, tmp_path, capsys):
    warehouse(FakeWarehouse(sample_tables()))
    assert _main(tmp_path, "--check-keys-only", env_text='verify_key_columns = { "cat.sch.payments" = "customer_id" }\n',
                 extra_tags=[("cat.sch.payments.customer_id", "class", "customer_identifier")]) == 0
    assert "Row-pairing key for cat.sch.payments: customer_id (verify_key_columns)" in capsys.readouterr().out


def test_dry_run_names_the_keys_it_will_pick(tmp_path, capsys):
    tfvars, account = _abac(tmp_path)
    env = _env_file(tmp_path, 'verify_key_columns = { "cat.sch.notes" = "note_id" }\n')
    assert main(["--from-tfvars", str(tfvars), "--account-tfvars", str(account), "--env-file", str(env)]) == 0
    out = capsys.readouterr().out
    assert "[column-mask] cat.sch.notes.free_text key='note_id'" in out
    assert "[column-mask] cat.sch.customers.ssn key=auto (primary key or an id-like column" in out


# ── 4. saving the proven map ────────────────────────────────────────────────

def _proof(tmp_path, proven=None, *, masked=None, passed=True, **payload):
    proven = proven or {}
    masked = sorted(proven) if masked is None else masked
    path = tmp_path / "generated" / ".verify_access.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps({
        "passed": passed, "masked_tables": masked,
        "mask_keys_complete": passed and set(masked) <= set(proven),
        "mask_checks_passed_by_key": {}, "mask_keys_proven_by_table": proven, **payload}))
    return path


PROVEN_MAP = {"cat.sch.customers": "customer_id", "cat.sch.notes": "note_id"}


def test_map_is_saved_only_after_every_masked_table_was_proven(tmp_path, capsys):
    env = _env_file(tmp_path, "# keep\nx = 1\n")
    for make_proof in (lambda: _proof(tmp_path, PROVEN_MAP, passed=False),
                       lambda: _proof(tmp_path, {"cat.sch.customers": "customer_id"}, masked=sorted(PROVEN_MAP)),
                       lambda: _proof(tmp_path, {}, masked=[]), lambda: None):
        saved_settings.save_verify_key(env, "", make_proof())
        assert env.read_text() == "# keep\nx = 1\n"
    assert "verify_key_columns not saved: verify-access did not prove every masked table's row-pairing " \
           "key (unproven: cat.sch.notes)" in capsys.readouterr().out
    # A hand-edited result that claims completeness for a partial map is not trusted either.
    bad = _proof(tmp_path, {"cat.sch.customers": "customer_id"}, masked=sorted(PROVEN_MAP))
    bad.write_text(bad.read_text().replace('"mask_keys_complete": false', '"mask_keys_complete": true'))
    saved_settings.save_verify_key(env, "", bad)
    assert env.read_text() == "# keep\nx = 1\n"

    saved_settings.save_verify_key(env, "", _proof(tmp_path, PROVEN_MAP))
    assert hcl2.loads(env.read_text())["verify_key_columns"] == PROVEN_MAP
    assert env.read_text().startswith("# keep\nx = 1\n")
    assert "Saved the proven row-pairing key per table as verify_key_columns" in capsys.readouterr().out
    saved_settings.save_verify_key(env, "", _proof(tmp_path, PROVEN_MAP))      # idempotent
    assert "Saved" not in capsys.readouterr().out
    assert "verify_key_column " not in env.read_text()   # no explicit key: the legacy setting is untouched


def test_saving_replaces_the_map_and_drops_only_its_own_stale_entries(tmp_path, capsys):
    env = _env_file(tmp_path, "")
    saved_settings.save_verify_key(env, "", _proof(tmp_path, PROVEN_MAP))
    # The user adds an override for a table this run no longer masks.
    env.write_text(env.read_text().replace('"cat.sch.notes" = "note_id"',
                                           '"cat.sch.notes" = "note_id"\n  "cat.sch.legacy" = "legacy_id"'))
    capsys.readouterr()
    # notes is no longer masked; payments is new.
    saved_settings.save_verify_key(env, "", _proof(tmp_path, {"cat.sch.customers": "customer_id",
                                                               "cat.sch.payments": "payment_id"}))
    out = capsys.readouterr().out
    assert hcl2.loads(env.read_text())["verify_key_columns"] == {
        "cat.sch.customers": "customer_id", "cat.sch.payments": "payment_id",
        "cat.sch.legacy": "legacy_id"}                       # the user's entry is kept...
    assert "removed stale cat.sch.notes" in out              # ...the one this tool saved is not
    assert "kept your verify_key_columns entry for cat.sch.legacy (legacy_id)" in out


def test_legacy_explicit_key_is_still_saved(tmp_path):
    env = _env_file(tmp_path, 'verify_key_column = ""\n')
    proof = _proof(tmp_path, {"cat.sch.customers": "customer_id"}, mask_checks_passed_by_key={"customer_id": 2})
    saved_settings.save_verify_key(env, "customer_id", proof)
    cfg = hcl2.loads(env.read_text())
    assert cfg["verify_key_column"] == "customer_id"
    assert cfg["verify_key_columns"] == {"cat.sch.customers": "customer_id"}


def _clean_env(**extra):
    env = {k: v for k, v in os.environ.items()
           if k not in ("GNUMAKEFLAGS", "MAKEFLAGS", "MAKELEVEL", "VERIFY_KEY_COLUMN", "VERIFY_SPEC")}
    env.update(extra)
    return env


def _stub_make(tmp_path, result_path, *, result=None, fail=None):
    log = tmp_path / "calls"
    stub = tmp_path / "make-stub"
    body = ["#!/bin/sh", f"printf '%s\\n' \"$*\" >> '{log}'"]
    if result is not None:
        body.append(f"""if [ "$1" = "verify-access" ]; then printf '%s' '{json.dumps(result)}' > '{result_path}'; fi""")
    if fail:
        body.append(f'[ "$1" = "{fail}" ] && exit 1')
    stub.write_text("\n".join(body + ["exit 0"]) + "\n")
    stub.chmod(0o755)
    return stub, log


def _run_make(tmp_path, target, env_name, *extra, result=None, fail=None, cloud="aws"):
    env_dir = tmp_path / env_name
    (env_dir / "generated").mkdir(parents=True, exist_ok=True)
    env_file = env_dir / "env.auto.tfvars"
    if not env_file.exists():
        env_file.write_text("enable_classification = true\n")
    stub, log = _stub_make(tmp_path, env_dir / "generated/.verify_access.json", result=result, fail=fail)
    account = tmp_path / "account"
    account.mkdir(exist_ok=True)
    (account / "abac.auto.tfvars").write_text("groups = { analysts = {}, ops = {} }\n")
    proc = subprocess.run(["make", "--no-print-directory", target, f"ENV={env_name}", f"ENV_DIR={env_dir}",
                           f"ACCOUNT_ENV_DIR={account}", f"MAKE={stub}", *extra],
                          cwd=ROOT / cloud, text=True, capture_output=True, env=_clean_env(), timeout=120)
    calls = [shlex.split(line) for line in log.read_text().splitlines()] if log.exists() else []
    return proc, env_file, calls


PROVEN = {"passed": True, "mask_checks_passed": 3, "mask_checks_passed_by_key": {"customer_id": 1},
          "masked_tables": ["dev_cat.sch.customers", "dev_cat.sch.notes"], "mask_keys_complete": True,
          "mask_keys_proven_by_table": {"dev_cat.sch.customers": "customer_id", "dev_cat.sch.notes": "note_id"},
          "row_filter_checks_passed": 0}


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_rehearse_with_no_key_saves_the_proven_map(tmp_path, cloud):
    proc, env_file, calls = _run_make(tmp_path, "rehearse", "dev", result=PROVEN, cloud=cloud)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert ["verify-access", "ENV=dev"] in calls                      # no key flag anywhere
    assert not any("VERIFY_KEY_COLUMN" in w for call in calls for w in call)
    assert hcl2.loads(env_file.read_text())["verify_key_columns"] == PROVEN["mask_keys_proven_by_table"]
    assert "verify_key_column " not in env_file.read_text()


@pytest.mark.parametrize("result, fail", [({**PROVEN, "passed": False}, None), (PROVEN, "verify-access")],
                         ids=["not-passed", "verify-failed"])
def test_rehearse_saves_no_map_without_a_passing_proof(tmp_path, result, fail):
    proc, env_file, _ = _run_make(tmp_path, "rehearse", "dev", result=result, fail=fail)
    assert env_file.read_text() == "enable_classification = true\n"


# ── 5. release: the admin key check runs before anything is applied ─────────

MASKED_CONFIG = '''fgac_policies = [
  { name = "mask_redact", policy_type = "POLICY_TYPE_COLUMN_MASK", catalog = "cat",
    to_principals = ["analysts"], match_condition = "hasTagValue('gr_treatment', 'redact')" },
]
tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.customers.ssn", tag_key = "gr_treatment", tag_value = "redact" },
]
'''


def _prod(tmp_path):
    env_dir = tmp_path / "prod"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "data_access/abac.auto.tfvars").write_text(MASKED_CONFIG)
    return env_dir


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_release_key_check_refuses_before_apply(tmp_path, cloud):
    _prod(tmp_path)
    proc, env_file, calls = _run_make(tmp_path, "release", "prod", fail="verify-access-keys", cloud=cloud)
    assert proc.returncode != 0
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in calls]
    assert names[-1] == "verify-access-keys", names
    assert "apply" not in names and "audit-rulebook" not in names and "verify-access" not in names
    assert "release: no provable row-pairing key for every masked table (above); nothing was applied." in proc.stderr
    assert env_file.read_text() == "enable_classification = true\n"


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_release_with_no_key_reaches_apply_after_the_key_check(tmp_path, cloud):
    _prod(tmp_path)
    proc, _env_file, calls = _run_make(tmp_path, "release", "prod", result=PROVEN, cloud=cloud)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    names = [c[1] if c[0] == "--no-print-directory" else c[0] for c in calls]
    assert names.index("verify-access-keys") < names.index("apply") < names.index("verify-access")
    assert ["verify-access-keys", "ENV=prod"] in calls
    assert ["verify-access", "ENV=prod", "VERIFY_REQUIRE_MASKS=1"] in calls


def _make_n(target, *extra, cloud="aws", env_dir=None):
    args = ["make", "-n", "--no-print-directory", target, *extra]
    return subprocess.run(args, cwd=ROOT / cloud, text=True, capture_output=True, env=_clean_env(), timeout=120)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_verify_access_keys_target_is_admin_only_and_reads_the_map(tmp_path, cloud):
    env_dir = tmp_path / "prod"
    env_dir.mkdir()
    (env_dir / "env.auto.tfvars").write_text("")
    proc = _make_n("verify-access-keys", "ENV=prod", f"ENV_DIR={env_dir}", cloud=cloud)
    assert proc.returncode == 0, proc.stderr
    line = next(l for l in proc.stdout.splitlines() if "verify_effective_access.py" in l)
    full = proc.stdout[proc.stdout.index(line):]
    assert "--check-keys-only" in full and f'--env-file "{env_dir}/env.auto.tfvars"' in full
    assert "--live" not in full.split("--check-keys-only")[0].split("verify_effective_access.py")[-1]
    assert "--allow-unset" in proc.stdout


def test_verify_access_passes_the_per_table_map():
    makefile = (SHARED / "Makefile.shared").read_text()
    body = makefile[makefile.index("\nverify-access:"):]
    body = body[:body.index("\n\n")]
    assert "$(_VERIFY_KEY_FLAGS)" in body
    assert '_VERIFY_KEY_FLAGS = --key-column "$$key" --env-file "$(ENV_DIR)/env.auto.tfvars"' in makefile


def test_resolve_warehouse_allow_unset_only_relaxes_the_unset_case(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    assert resolve_env_config.resolve_warehouse(tmp_path, allow_unset=True) == ""
    with pytest.raises(ValueError, match="no SQL warehouse could be resolved"):
        resolve_env_config.resolve_warehouse(tmp_path)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ sql_warehouse_id = "a" }, { sql_warehouse_id = "b" }]\n')
    with pytest.raises(ValueError, match="ambiguous"):
        resolve_env_config.resolve_warehouse(tmp_path, allow_unset=True)


def test_release_mask_proof_needs_no_key_but_still_proves_every_masked_column(tmp_path, capsys):
    env_dir = _prod(tmp_path)
    (env_dir / "env.auto.tfvars").write_text("")
    assert rh.require_mask_proof(env_dir, "prod", "", "") == 0
    account = tmp_path / "account.auto.tfvars"
    account.write_text("groups = {}\n")
    (env_dir / "data_access/abac.auto.tfvars").write_text(
        MASKED_CONFIG.replace('to_principals = ["analysts"]', 'to_principals = ["account users"]'))
    assert rh.require_mask_proof(env_dir, "prod", "", "", account) == 1
    assert "cat.sch.customers.ssn" in capsys.readouterr().err


# ── 6. promote remaps the map ───────────────────────────────────────────────

def _promote(tmp_path, monkeypatch, source_text, dest_text=None):
    source, dest = tmp_path / "dev", tmp_path / "prod"
    source.mkdir()
    (source / "env.auto.tfvars").write_text('uc_tables = ["dev_cat.sch.customers"]\n' + source_text)
    if dest_text is not None:
        dest.mkdir()
        (dest / "env.auto.tfvars").write_text(dest_text)
    monkeypatch.setattr(sys, "argv", ["remap_env_config.py", str(source), str(dest), "dev_cat=prod_cat"])
    remap_env_config.main()
    return hcl2.loads((dest / "env.auto.tfvars").read_text())


def test_promote_remaps_table_names_in_the_map(tmp_path, monkeypatch):
    cfg = _promote(tmp_path, monkeypatch,
                   'verify_key_columns = {\n  "dev_cat.sch.customers" = "customer_id"\n'
                   '  "other.sch.t" = "t_id"\n}\n',
                   dest_text='verify_key_columns = { "prod_cat.sch.old" = "x" }\n')
    assert cfg["verify_key_columns"] == {"prod_cat.sch.customers": "customer_id", "other.sch.t": "t_id"}


def test_promote_keeps_the_destination_map_when_the_source_has_none(tmp_path, monkeypatch):
    cfg = _promote(tmp_path, monkeypatch, "",
                   dest_text='verify_key_columns = { "prod_cat.sch.customers" = "prod_key" }\n')
    assert cfg["verify_key_columns"] == {"prod_cat.sch.customers": "prod_key"}


def test_promote_writes_no_map_when_neither_env_has_one(tmp_path, monkeypatch):
    assert "verify_key_columns" not in _promote(tmp_path, monkeypatch, "")


# ── 7. Terraform: the setting is declared where env.auto.tfvars is read ─────

@pytest.mark.parametrize("root", ["data_access", "workspace"])
def test_verify_key_columns_is_a_declared_ignored_variable(root):
    text = (SHARED / "roots" / root / "main.tf").read_text()
    block = text[text.index('variable "verify_key_columns"'):]
    block = block[:block.index("}\n") + 2]
    assert "type        = map(string)" in block and "default     = {}" in block
    assert "var.verify_key_columns" not in text   # tooling only; Terraform ignores it


@pytest.mark.parametrize("path", [
    "shared/env.auto.tfvars.example", "shared/examples/dev_to_prod/env.auto.tfvars.example"])
def test_examples_document_the_optional_override(path):
    text = (ROOT / path).read_text()
    assert "verify_key_columns" in text
    for line in text.splitlines():
        if "verify_key_column" in line:
            assert line.lstrip().startswith("#"), line   # optional: never set by default
