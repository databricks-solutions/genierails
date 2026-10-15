#!/usr/bin/env python3
"""Tests for the local-vs-prod Genie space diff.

Run: python3 tests/test_space_diff.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import space_diff  # noqa: E402


class TestNoDifference(unittest.TestCase):
    def test_identical_payloads(self):
        payload = {"data_sources": [{"table_name": "c.s.t"}], "instructions": ["a"]}
        self.assertEqual(space_diff.diff(payload, dict(payload)), [])


class TestTableDiff(unittest.TestCase):
    def test_added_table(self):
        lines = space_diff.diff(
            {"data_sources": [{"table_name": "c.s.a"}, {"table_name": "c.s.b"}]},
            {"data_sources": [{"table_name": "c.s.a"}]},
        )
        self.assertIn("  + table c.s.b", lines)

    def test_removed_table_is_flagged_as_prod_only(self):
        lines = space_diff.diff(
            {"data_sources": [{"table_name": "c.s.a"}]},
            {"data_sources": [{"table_name": "c.s.a"}, {"table_name": "c.s.b"}]},
        )
        self.assertTrue(any("c.s.b" in line and line.lstrip().startswith("-")
                            for line in lines))
        self.assertEqual(len(space_diff.prod_only_changes(lines)), 1)

    def test_same_names_different_details(self):
        lines = space_diff.diff(
            {"data_sources": [{"table_name": "c.s.a", "description": "new"}]},
            {"data_sources": [{"table_name": "c.s.a", "description": "old"}]},
        )
        self.assertTrue(any("same table names" in line for line in lines))

    def test_plain_string_table_list(self):
        lines = space_diff.diff(
            {"tables": ["c.s.a", "c.s.b"]}, {"tables": ["c.s.a"]}
        )
        self.assertIn("  + table c.s.b", lines)

    def test_alternate_key_name(self):
        """Schema variation: full_name instead of table_name."""
        lines = space_diff.diff(
            {"data_sources": [{"full_name": "c.s.b"}]},
            {"data_sources": [{"full_name": "c.s.a"}]},
        )
        self.assertIn("  + table c.s.b", lines)

    def test_real_nested_shape(self):
        """The shape a live workspace returns: data_sources.tables[].identifier."""
        def payload(*identifiers):
            return {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": name,
                            "column_configs": [
                                {"column_name": "id",
                                 "enable_format_assistance": True}
                            ],
                        }
                        for name in identifiers
                    ]
                }
            }

        lines = space_diff.diff(
            payload("main.gold.customer_summary"),
            payload("main.gold.customer_summary", "main.gold.legacy"),
        )
        self.assertTrue(any("main.gold.legacy" in line
                            and line.lstrip().startswith("-")
                            for line in lines))
        self.assertEqual(len(space_diff.prod_only_changes(lines)), 1)

    def test_nested_shape_extracts_all_identifiers(self):
        node = {"tables": [{"identifier": "a.b.c"}, {"identifier": "d.e.f"}]}
        self.assertEqual(space_diff._table_names(node), {"a.b.c", "d.e.f"})

    def test_column_configs_do_not_leak_as_table_names(self):
        """A nested column_name must not be mistaken for a table."""
        node = {
            "tables": [
                {"identifier": "a.b.c",
                 "column_configs": [{"column_name": "not_a_table"}]}
            ]
        }
        self.assertEqual(space_diff._table_names(node), {"a.b.c"})


class TestTextDiff(unittest.TestCase):
    def test_added_instruction(self):
        lines = space_diff.diff(
            {"instructions": ["keep it simple", "filter by region"]},
            {"instructions": ["keep it simple"]},
        )
        self.assertTrue(any("filter by region" in line for line in lines))

    def test_prod_only_instruction_warns(self):
        lines = space_diff.diff(
            {"instructions": ["mine"]},
            {"instructions": ["mine", "added in the prod UI"]},
        )
        lost = space_diff.prod_only_changes(lines)
        self.assertEqual(len(lost), 1)
        self.assertIn("will be lost", lost[0])

    def test_dict_shaped_instructions(self):
        lines = space_diff.diff(
            {"instructions": [{"type": "text", "content": "new rule"}]},
            {"instructions": [{"type": "text", "content": "old rule"}]},
        )
        self.assertTrue(any("new rule" in line for line in lines))
        self.assertEqual(len(space_diff.prod_only_changes(lines)), 1)

    def test_id_field_ignored(self):
        """A server-assigned id must not read as a content change."""
        lines = space_diff.diff(
            {"instructions": [{"id": "1", "content": "same"}]},
            {"instructions": [{"id": "2", "content": "same"}]},
        )
        self.assertEqual(lines, [])

    def test_long_text_is_clipped(self):
        lines = space_diff.diff(
            {"instructions": ["x" * 500]}, {"instructions": []}
        )
        self.assertTrue(all(len(line) < 200 for line in lines))


class TestScalarDiff(unittest.TestCase):
    def test_scalar_change_shows_both_sides(self):
        lines = space_diff.diff({"version": 3}, {"version": 2})
        self.assertTrue(any("2 -> 3" in line for line in lines))

    def test_absent_locally(self):
        lines = space_diff.diff({}, {"title": "Prod Title"})
        self.assertTrue(any("(absent)" in line for line in lines))

    def test_unknown_nested_key(self):
        lines = space_diff.diff(
            {"mystery": {"a": 1}}, {"mystery": {"a": 2}}
        )
        self.assertIn("  ~ mystery changed", lines)


class TestProdOnlyChanges(unittest.TestCase):
    def test_empty_when_only_additions(self):
        lines = ["  + table c.s.b", "  ~ version: 2 -> 3"]
        self.assertEqual(space_diff.prod_only_changes(lines), [])

    def test_picks_only_minus_lines(self):
        lines = ["  + table a", "  - table b (present in prod, not in yours)"]
        self.assertEqual(len(space_diff.prod_only_changes(lines)), 1)


class TestNonDictPayloads(unittest.TestCase):
    def test_non_dict_remote_does_not_crash(self):
        self.assertIsInstance(space_diff.diff({"a": 1}, "unexpected"), list)

    def test_none_remote(self):
        self.assertIsInstance(space_diff.diff({"a": 1}, None), list)


if __name__ == "__main__":
    unittest.main(verbosity=2)
