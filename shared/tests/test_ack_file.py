import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import ack_file


def test_parse_every_supported_kind_and_comments():
    entries = ack_file.parse_text("""
      # reviewed exceptions
      weaken:cat.sch.tab.col:etl-sp
      unclassified:cat.sch.tab.col
      reader:cat.sch.tab:BI users
      rollback:0123456789abcdef
      revoke:cat.sch.tab:old-reader
    """)
    assert [entry.key for entry in entries] == [
        "weaken:cat.sch.tab.col:etl-sp",
        "unclassified:cat.sch.tab.col",
        "reader:cat.sch.tab:BI users",
        "rollback:0123456789abcdef",
        "revoke:cat.sch.tab:old-reader",
    ]


@pytest.mark.parametrize("line, message", [
    ("surprise:cat.sch.tab", "unknown acknowledgement kind"),
    ("weaken:cat.sch.tab:p", "4 valid identifier parts"),
    ("reader:cat.sch.tab.col:p", "3 valid identifier parts"),
    ("unclassified:cat.sch.tab.col:p", "expected unclassified"),
    ("rollback:not-a-sha", "7-64 hex commit"),
    ("reader:cat.sch.tab:", "invalid principal"),
    ("reader:cat.sch.tab:p # why", "inline comments"),
])
def test_strict_validation(line, message):
    with pytest.raises(ack_file.AckFileError, match=message):
        ack_file.parse_text(line)


def test_duplicate_entries_are_refused():
    with pytest.raises(ack_file.AckFileError, match="duplicate.*first declared"):
        ack_file.parse_text("reader:c.s.t:p\nreader:c.s.t:p\n")


def test_reconcile_reports_entries_matching_nothing():
    entries = ack_file.parse_text("reader:c.s.t:p\nrollback:abcdef0\n")
    matched, unmatched = ack_file.reconcile(entries, {"reader:c.s.t:p": "no read -> SELECT"})
    assert matched == {"reader:c.s.t:p": "no read -> SELECT"}
    assert [entry.key for entry in unmatched] == ["rollback:abcdef0"]


def test_cli_refuses_unmatched_ack(tmp_path, capsys):
    ack = tmp_path / "ack.txt"; ack.write_text("reader:c.s.t:p\n")
    consequences = tmp_path / "consequences.json"; consequences.write_text(json.dumps({}))
    assert ack_file.main([str(ack), "--consequences", str(consequences)]) == 1
    assert "matches nothing" in capsys.readouterr().err


def test_cli_syntax_error_is_distinct(tmp_path, capsys):
    ack = tmp_path / "ack.txt"; ack.write_text("wat:c.s.t\n")
    assert ack_file.main([str(ack)]) == 2
    assert "unknown acknowledgement kind" in capsys.readouterr().err
