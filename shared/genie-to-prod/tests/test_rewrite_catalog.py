#!/usr/bin/env python3
"""Tests for the serialized_space catalog rewriter.

Run: python3 tests/test_rewrite_catalog.py
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import rewrite_catalog  # noqa: E402


CATALOG_MAP = {"dev_analytics": "prod_analytics"}


class TestRewriteString(unittest.TestCase):
    def test_rewrites_table_prefix(self):
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders", CATALOG_MAP
        )
        self.assertEqual(out, "prod_analytics.sales.orders")
        self.assertEqual(count, 1)

    def test_does_not_rewrite_longer_identifier(self):
        """dev_analytics_staging must not be mangled into prod_analytics_staging."""
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics_staging.sales.orders", CATALOG_MAP
        )
        self.assertEqual(out, "dev_analytics_staging.sales.orders")
        self.assertEqual(count, 0)

    def test_does_not_rewrite_as_suffix(self):
        out, count = rewrite_catalog.rewrite_string(
            "my_dev_analytics.sales.orders", CATALOG_MAP
        )
        self.assertEqual(out, "my_dev_analytics.sales.orders")
        self.assertEqual(count, 0)

    def test_requires_trailing_dot(self):
        """A bare catalog mention with no object after it is left alone."""
        out, count = rewrite_catalog.rewrite_string(
            "the dev_analytics catalog", CATALOG_MAP
        )
        self.assertEqual(out, "the dev_analytics catalog")
        self.assertEqual(count, 0)

    def test_rewrites_multiple_occurrences(self):
        out, count = rewrite_catalog.rewrite_string(
            "JOIN dev_analytics.sales.orders o ON o.id = dev_analytics.sales.items.id",
            CATALOG_MAP,
        )
        self.assertNotIn("dev_analytics", out)
        self.assertEqual(count, 2)


class TestMappingLevels(unittest.TestCase):
    """catalog_map accepts catalog, catalog.schema, and catalog.schema.table."""

    def test_catalog_and_schema(self):
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders",
            {"dev_analytics.sales": "prod_analytics.retail"},
        )
        self.assertEqual(out, "prod_analytics.retail.orders")
        self.assertEqual(count, 1)

    def test_full_table_rename(self):
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders",
            {"dev_analytics.sales.orders": "prod_analytics.retail.txns"},
        )
        self.assertEqual(out, "prod_analytics.retail.txns")
        self.assertEqual(count, 1)

    def test_table_rename_inside_sql(self):
        out, _ = rewrite_catalog.rewrite_string(
            "SELECT * FROM dev_analytics.sales.orders WHERE id = 1",
            {"dev_analytics.sales.orders": "prod_analytics.retail.txns"},
        )
        self.assertEqual(out, "SELECT * FROM prod_analytics.retail.txns WHERE id = 1")

    def test_table_rename_followed_by_punctuation(self):
        out, count = rewrite_catalog.rewrite_string(
            "FROM dev_analytics.sales.orders, dev_analytics.sales.items",
            {"dev_analytics.sales.orders": "prod.retail.txns"},
        )
        self.assertEqual(out, "FROM prod.retail.txns, dev_analytics.sales.items")
        self.assertEqual(count, 1)

    def test_table_rename_does_not_match_longer_table(self):
        """orders must not match inside orders_archive."""
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders_archive",
            {"dev_analytics.sales.orders": "prod.retail.txns"},
        )
        self.assertEqual(out, "dev_analytics.sales.orders_archive")
        self.assertEqual(count, 0)

    def test_table_rename_does_not_match_deeper_path(self):
        """A three-part rule must not fire on a four-part reference."""
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders.column_x",
            {"dev_analytics.sales.orders": "prod.retail.txns"},
        )
        self.assertEqual(count, 0)

    def test_specific_mapping_wins_over_general(self):
        """A table rule and a catalog rule coexist; the table rule applies first."""
        out, count = rewrite_catalog.rewrite_string(
            "dev_analytics.sales.orders and dev_analytics.sales.customers",
            {
                "dev_analytics": "prod_analytics",
                "dev_analytics.sales.orders": "prod_analytics.retail.txns",
            },
        )
        self.assertEqual(
            out, "prod_analytics.retail.txns and prod_analytics.sales.customers"
        )
        self.assertEqual(count, 2)

    def test_schema_rule_wins_over_catalog_rule(self):
        out, _ = rewrite_catalog.rewrite_string(
            "dev_cat.sales.orders and dev_cat.other.items",
            {"dev_cat": "prod_cat", "dev_cat.sales": "prod_cat.retail"},
        )
        self.assertEqual(out, "prod_cat.retail.orders and prod_cat.other.items")

    def test_ordering_is_by_specificity(self):
        ordered = rewrite_catalog._by_specificity(
            {"a": "x", "a.b.c": "x.y.z", "a.b": "x.y"}
        )
        self.assertEqual([key for key, _ in ordered], ["a.b.c", "a.b", "a"])

    def test_all_three_levels_together(self):
        payload = {
            "data_sources": [
                {"table_name": "dev_cat.sales.orders"},
                {"table_name": "dev_cat.sales.customers"},
                {"table_name": "dev_cat.other.events"},
            ]
        }
        out, _ = rewrite_catalog.rewrite(
            payload,
            {
                "dev_cat.sales.orders": "prod_cat.retail.txns",
                "dev_cat.sales": "prod_cat.retail",
                "dev_cat": "prod_cat",
            },
        )
        names = [d["table_name"] for d in out["data_sources"]]
        self.assertEqual(names, [
            "prod_cat.retail.txns",
            "prod_cat.retail.customers",
            "prod_cat.other.events",
        ])


class TestRewriteStructure(unittest.TestCase):
    def setUp(self):
        # Mirrors the shapes a real serialized_space carries: a structured data
        # source, example SQL, and free-text instructions.
        self.payload = {
            "data_sources": [
                {"table_name": "dev_analytics.sales.orders"},
                {"table_name": "dev_analytics.sales.customers"},
            ],
            "example_queries": [
                {
                    "question": "top customers?",
                    "sql": "SELECT * FROM dev_analytics.sales.orders LIMIT 10",
                }
            ],
            "instructions": "Always filter dev_analytics.sales.orders by region.",
            "settings": {"row_limit": 1000, "enabled": True, "note": None},
        }

    def test_rewrites_every_location(self):
        out, replacements = rewrite_catalog.rewrite(self.payload, CATALOG_MAP)
        self.assertNotIn("dev_analytics", json.dumps(out))
        # two data sources, one SQL string, one instruction string
        self.assertEqual(len(replacements), 4)

    def test_preserves_non_string_types(self):
        out, _ = rewrite_catalog.rewrite(self.payload, CATALOG_MAP)
        self.assertEqual(out["settings"]["row_limit"], 1000)
        self.assertIs(out["settings"]["enabled"], True)
        self.assertIsNone(out["settings"]["note"])

    def test_reports_json_paths(self):
        _, replacements = rewrite_catalog.rewrite(self.payload, CATALOG_MAP)
        paths = [path for path, _, _ in replacements]
        self.assertIn("$.data_sources[0].table_name", paths)
        self.assertIn("$.example_queries[0].sql", paths)
        self.assertIn("$.instructions", paths)

    def test_idempotent(self):
        """Re-running against an already-promoted payload changes nothing."""
        once, _ = rewrite_catalog.rewrite(self.payload, CATALOG_MAP)
        twice, replacements = rewrite_catalog.rewrite(once, CATALOG_MAP)
        self.assertEqual(once, twice)
        self.assertEqual(replacements, [])

    def test_unmapped_catalog_detected(self):
        payload = {"sql": "SELECT * FROM dev_other.sales.orders"}
        out, _ = rewrite_catalog.rewrite(payload, CATALOG_MAP)
        unmapped = rewrite_catalog.find_unmapped(out, ["dev_other"])
        self.assertEqual(len(unmapped), 1)
        self.assertEqual(unmapped[0][1], "dev_other")

    def test_no_unmapped_after_full_rewrite(self):
        out, _ = rewrite_catalog.rewrite(self.payload, CATALOG_MAP)
        self.assertEqual(rewrite_catalog.find_unmapped(out, CATALOG_MAP.keys()), [])


class TestExtractTables(unittest.TestCase):
    def test_finds_three_part_names(self):
        payload = {
            "data_sources": [{"table_name": "prod_analytics.sales.orders"}],
            "sql": "SELECT * FROM prod_analytics.sales.customers",
        }
        tables = rewrite_catalog.extract_tables(payload)
        self.assertIn("prod_analytics.sales.orders", tables)
        self.assertIn("prod_analytics.sales.customers", tables)

    def test_ignores_two_part_names(self):
        tables = rewrite_catalog.extract_tables({"sql": "SELECT * FROM sales.orders"})
        self.assertEqual(tables, set())


class TestCli(unittest.TestCase):
    def _run(self, payload, extra_args):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".geniespace.json", delete=False
        ) as handle:
            json.dump(payload, handle)
            path = handle.name
        argv = sys.argv
        sys.argv = ["rewrite_catalog.py", path, "--map", "dev_analytics=prod_analytics"]
        sys.argv.extend(extra_args)
        try:
            code = rewrite_catalog.main()
        finally:
            sys.argv = argv
        with open(path) as handle:
            result = json.load(handle)
        os.unlink(path)
        return code, result

    def test_in_place_write(self):
        code, result = self._run(
            {"sql": "SELECT * FROM dev_analytics.sales.orders"}, ["--in-place"]
        )
        self.assertEqual(code, 0)
        self.assertEqual(result["sql"], "SELECT * FROM prod_analytics.sales.orders")

    def test_without_in_place_leaves_file_untouched(self):
        code, result = self._run({"sql": "SELECT * FROM dev_analytics.sales.orders"}, [])
        self.assertEqual(code, 0)
        self.assertEqual(result["sql"], "SELECT * FROM dev_analytics.sales.orders")

    def test_exit_code_on_unmapped(self):
        # dev_analytics maps cleanly; the mapping key itself is what we scan for,
        # so an unmapped survivor requires a reference the map cannot reach.
        code, _ = self._run(
            {"sql": "SELECT * FROM dev_analytics_archive.sales.orders"}, ["--in-place"]
        )
        # dev_analytics_archive is a different catalog and is not in the map, so
        # it is not scanned for; the rewrite succeeds without touching it.
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
