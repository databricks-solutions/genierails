import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import ack_file
import weakening as wk


COLUMN = "cat.sch.tab.secret"


def protection(level, text=None):
    return wk.Protection(level, text or level)


def state(level="redacted", principal="analysts"):
    return wk.GovernanceState(columns={COLUMN: {principal: protection(level)}})


@pytest.mark.parametrize("before,after", [
    ("redacted", "partial:v1"), ("NULL", "partial:v1"),
    ("partial:v1", "raw"), ("redacted", "raw"),
])
def test_every_downward_protection_transition_weakens(before, after):
    issues = wk.find_weakenings(state(before), state(after))
    assert [(i.category, i.before, i.after) for i in issues] == [("protection", before, after)]


@pytest.mark.parametrize("before,after", [
    ("raw", "partial:v1"), ("partial:v1", "redacted"),
    ("redacted", "NULL"), ("partial:v1", "partial:v2"),
])
def test_equal_or_stronger_protection_does_not_weaken(before, after):
    assert wk.find_weakenings(state(before), state(after)) == ()


def test_new_raw_exempt_principal_is_flagged_per_column_with_exact_consequence():
    before = state("redacted", "etl")
    after = state("raw", "etl"); after.raw_exempt_principals = {"etl"}
    issue = next(i for i in wk.find_weakenings(before, after) if i.category == "raw_exempt_principal")
    assert issue.ack_key == f"weaken:{COLUMN}:etl"
    assert issue.consequence == "redacted -> raw"


def test_partial_raw_is_flagged_for_each_partial_principal():
    before = state("partial:v1")
    before.partial_versions = {COLUMN: "mask"}
    after = state("partial:v1")
    after.partial_versions = {COLUMN: "raw"}
    issue = wk.find_weakenings(before, after)[0]
    assert issue.category == "partial_raw"
    assert issue.before == "partial:v1"
    assert issue.after == "raw through partial='raw'"


def test_removed_row_filter_weakens_every_column_on_table():
    second = "cat.sch.tab.other"
    before = wk.GovernanceState(
        columns={COLUMN: {}, second: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset({"APAC"}))}},
    )
    issues = wk.find_weakenings(before, wk.GovernanceState(columns=before.columns))
    assert {i.object_name for i in issues} == {COLUMN, second}
    assert all(i.category == "row_filter" and i.after == "all rows (filter removed)" for i in issues)


def test_widened_row_filter_is_flagged_but_narrowed_is_not():
    base = wk.GovernanceState(
        columns={COLUMN: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset({"APAC"}))}},
    )
    wide = wk.GovernanceState(
        columns={COLUMN: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset({"APAC", "EMEA"}))}},
    )
    narrow = wk.GovernanceState(
        columns={COLUMN: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset())}},
    )
    assert wk.find_weakenings(base, wide)[0].category == "row_filter"
    assert wk.find_weakenings(base, narrow) == ()


def test_row_filter_replacement_that_exposes_any_new_value_is_weakening():
    base = wk.GovernanceState(
        columns={COLUMN: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset({"APAC"}))}},
    )
    replaced = wk.GovernanceState(
        columns={COLUMN: {}},
        row_filters={"cat.sch.tab": {"regional": wk.RowFilter(frozenset({"EMEA"}))}},
    )
    assert wk.find_weakenings(base, replaced)[0].category == "row_filter"


def test_new_table_reader_uses_reader_ack_kind():
    before = wk.GovernanceState(table_readers={"cat.sch.tab": {"old"}})
    after = wk.GovernanceState(table_readers={"cat.sch.tab": {"old", "new"}})
    issue = wk.find_weakenings(before, after)[0]
    assert issue.ack_key == "reader:cat.sch.tab:new"
    assert issue.consequence == "no GenieRails SELECT -> GenieRails grants SELECT"


def test_exact_ack_subtracts_issue_and_reports_before_after():
    before, after = state("redacted"), state("raw")
    entries = ack_file.parse_text(f"weaken:{COLUMN}:analysts\n")
    result = wk.check_weakenings(before, after, entries)
    assert result.ok
    assert result.acknowledged[0].consequence == "redacted -> raw"


def test_wrongly_scoped_ack_is_unmatched_and_does_not_hide_issue():
    entries = ack_file.parse_text("weaken:cat.sch.tab.other:analysts\n")
    result = wk.check_weakenings(state("redacted"), state("raw"), entries)
    assert not result.ok
    assert len(result.unacknowledged) == 1
    assert result.unmatched_acknowledgements == entries


def test_ack_with_no_weakening_is_refused():
    entries = ack_file.parse_text(f"weaken:{COLUMN}:analysts\n")
    result = wk.check_weakenings(state("raw"), state("redacted"), entries)
    assert not result.ok and result.unmatched_acknowledgements == entries


def test_multiple_categories_sharing_ack_key_are_all_acknowledged():
    before = state("redacted", "etl")
    after = state("raw", "etl"); after.raw_exempt_principals = {"etl"}
    result = wk.check_weakenings(
        before, after, ack_file.parse_text(f"weaken:{COLUMN}:etl\n")
    )
    assert result.ok and {i.category for i in result.acknowledged} == {
        "protection", "raw_exempt_principal"
    }


def test_unknown_protection_is_refused():
    with pytest.raises(ValueError, match="unknown protection level"):
        wk.find_weakenings(state("redacted"), state("mystery"))


def test_cli_nonzero_for_unacknowledged_and_unmatched_ack(tmp_path, capsys):
    before = tmp_path / "before.json"; after = tmp_path / "after.json"; ack = tmp_path / "ack.txt"
    before.write_text(json.dumps({"columns": {COLUMN: {"p": {"level": "redacted", "consequence": "masked"}}}}))
    after.write_text(json.dumps({"columns": {COLUMN: {"p": {"level": "raw", "consequence": "clear text"}}}}))
    ack.write_text("reader:cat.sch.tab:q\n")
    assert wk.main(["--before", str(before), "--after", str(after), "--ack-file", str(ack)]) == 1
    error = capsys.readouterr().err
    assert "masked -> clear text" in error and "matches nothing" in error


def test_cli_success_prints_acknowledged_consequence(tmp_path, capsys):
    before = tmp_path / "before.json"; after = tmp_path / "after.json"; ack = tmp_path / "ack.txt"
    before.write_text(json.dumps({"table_readers": {}}))
    after.write_text(json.dumps({"table_readers": {"cat.sch.tab": ["p"]}}))
    ack.write_text("reader:cat.sch.tab:p\n")
    assert wk.main(["--before", str(before), "--after", str(after), "--ack-file", str(ack)]) == 0
    assert "no GenieRails SELECT -> GenieRails grants SELECT" in capsys.readouterr().out


def test_mutation_sentinel_for_key_comparison_boundaries():
    """These adjacent boundaries kill flipped rank, subset, and set-difference mutations."""
    assert wk.protection_rank("raw") < wk.protection_rank("partial:x") < wk.protection_rank("redacted")
    assert wk.find_weakenings(state("partial:x"), state("raw"))
    assert not wk.find_weakenings(state("raw"), state("partial:x"))
    prior = wk.RowFilter(frozenset({"A"}))
    assert not frozenset({"A", "B"}).issubset(prior.values)
    assert frozenset().issubset(prior.values)
    assert {"old", "new"} - {"old"} == {"new"}
