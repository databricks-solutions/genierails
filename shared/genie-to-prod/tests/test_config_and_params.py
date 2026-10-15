#!/usr/bin/env python3
"""Tests for the config reader and the YAML parameterizer.

The config reader has a no-PyYAML fallback path that must behave identically to
the PyYAML path, so each parser test runs against the fallback explicitly.

Run: python3 tests/test_config_and_params.py
"""

import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import parameterize  # noqa: E402
import read_config  # noqa: E402


SPACES_YML = """\
catalog_map:
  dev_analytics: prod_analytics
  dev_finance: prod_finance   # trailing comment

spaces:
  - key: sales_genie
    dev_id: 01f0aaa
    # title in dev: Sales
  - key: finance_genie
    dev_id: 01f0bbb
    prod_id: 01f1ccc
"""

PER_SPACE_YML = """\
catalog_map:
  dev_default: prod_default

spaces:
  - key: sales_genie
    dev_id: 01f0aaa
    catalog_map:
      dev_sales: prod_sales
      dev_sales.orders.detail: prod_sales.retail.txns
  - key: finance_genie
    dev_id: 01f0bbb
    prod_id: 01f1ccc
"""

DATABRICKS_YML = """\
bundle:
  name: genie-prod-promotion

targets:
  dev:
    mode: development
    variables:
      warehouse_id: ""
      parent_path: /Workspace/Users/a@b.com/genie
  prod:
    mode: production
    variables:
      warehouse_id: "abc123def456"
      parent_path: /Workspace/Shared/genie
"""


def write_temp(content, suffix=".yml"):
    handle = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
    handle.write(content)
    handle.close()
    return handle.name


class ConfigReaderMixin:
    """Runs each assertion under both the PyYAML and the fallback parser."""

    fallback = False

    def _maybe_no_yaml(self):
        if not self.fallback:
            return mock.patch.dict(sys.modules, {})
        # Force the ImportError branch in _load_with_pyyaml.
        return mock.patch.dict(sys.modules, {"yaml": None})

    def test_parses_spaces(self):
        path = write_temp(SPACES_YML)
        with self._maybe_no_yaml():
            spaces = read_config.parse_spaces(path)
        os.unlink(path)

        self.assertEqual(len(spaces), 2)
        self.assertEqual(spaces[0]["key"], "sales_genie")
        self.assertEqual(spaces[0]["dev_id"], "01f0aaa")
        self.assertEqual(spaces[0]["prod_id"], "")
        self.assertEqual(spaces[1]["key"], "finance_genie")
        self.assertEqual(spaces[1]["prod_id"], "01f1ccc")

    def test_parses_catalog_map(self):
        path = write_temp(SPACES_YML)
        with self._maybe_no_yaml():
            pairs = read_config.parse_catalog_map(path)
        os.unlink(path)

        self.assertIn("dev_analytics=prod_analytics", pairs)
        self.assertIn("dev_finance=prod_finance", pairs)
        self.assertEqual(len(pairs), 2)

    def test_per_space_catalog_map(self):
        """A space's own mapping is read, and does not leak to other spaces."""
        path = write_temp(PER_SPACE_YML)
        with self._maybe_no_yaml():
            spaces = read_config.parse_spaces(path)
        os.unlink(path)

        by_key = {s["key"]: s for s in spaces}
        self.assertEqual(
            by_key["sales_genie"]["catalog_map"],
            {"dev_sales": "prod_sales",
             "dev_sales.orders.detail": "prod_sales.retail.txns"},
        )
        # finance_genie defines none, so it falls back to the global map.
        self.assertEqual(by_key["finance_genie"]["catalog_map"], {})
        self.assertEqual(by_key["finance_genie"]["prod_id"], "01f1ccc")

    def test_nested_map_does_not_break_later_fields(self):
        """prod_id after a nested catalog_map must still parse."""
        content = (
            "spaces:\n"
            "  - key: a\n"
            "    dev_id: x\n"
            "    catalog_map:\n"
            "      c1: c2\n"
            "  - key: b\n"
            "    dev_id: y\n"
            "    prod_id: z\n"
        )
        path = write_temp(content)
        with self._maybe_no_yaml():
            spaces = read_config.parse_spaces(path)
        os.unlink(path)

        self.assertEqual(len(spaces), 2)
        self.assertEqual(spaces[0]["catalog_map"], {"c1": "c2"})
        self.assertEqual(spaces[1]["key"], "b")
        self.assertEqual(spaces[1]["prod_id"], "z")
        self.assertEqual(spaces[1]["catalog_map"], {})

    def test_global_map_excludes_nested_entries(self):
        """The top-level catalog_map must not absorb a space's nested one."""
        path = write_temp(PER_SPACE_YML)
        with self._maybe_no_yaml():
            pairs = read_config.parse_catalog_map(path)
        os.unlink(path)

        self.assertEqual(pairs, ["dev_default=prod_default"])

    def test_parses_prod_warehouse(self):
        path = write_temp(DATABRICKS_YML)
        with self._maybe_no_yaml():
            warehouse = read_config.parse_warehouse(path, "prod")
        os.unlink(path)
        self.assertEqual(warehouse, "abc123def456")

    def test_parses_dev_warehouse_as_empty(self):
        path = write_temp(DATABRICKS_YML)
        with self._maybe_no_yaml():
            warehouse = read_config.parse_warehouse(path, "dev")
        os.unlink(path)
        self.assertEqual(warehouse, "")


class TestConfigReaderPyYaml(ConfigReaderMixin, unittest.TestCase):
    fallback = False


class TestConfigReaderFallback(ConfigReaderMixin, unittest.TestCase):
    fallback = True

    def test_fallback_is_actually_exercised(self):
        """Guard: confirm the fallback path runs when yaml is unavailable."""
        path = write_temp(SPACES_YML)
        with mock.patch.dict(sys.modules, {"yaml": None}):
            self.assertIsNone(read_config._load_with_pyyaml(path))
        os.unlink(path)

    def test_comment_only_lines_ignored(self):
        content = "spaces:\n  # just a comment\n  - key: a\n    dev_id: x\n"
        path = write_temp(content)
        with mock.patch.dict(sys.modules, {"yaml": None}):
            spaces = read_config.parse_spaces(path)
        os.unlink(path)
        self.assertEqual(len(spaces), 1)
        self.assertEqual(spaces[0]["key"], "a")

    def test_dotted_mapping_keys_survive(self):
        """Regression: a dotted key was silently dropped, so a table-level
        mapping vanished on machines without PyYAML and the space deployed
        still pointing at dev tables."""
        content = (
            "catalog_map:\n"
            "  dev_cat.sales.orders: prod_cat.retail.txns\n"
            "  dev_cat.sales: prod_cat.retail\n"
            "  dev_cat: prod_cat\n"
        )
        path = write_temp(content)
        with mock.patch.dict(sys.modules, {"yaml": None}):
            pairs = read_config.parse_catalog_map(path)
        os.unlink(path)

        self.assertIn("dev_cat.sales.orders=prod_cat.retail.txns", pairs)
        self.assertIn("dev_cat.sales=prod_cat.retail", pairs)
        self.assertIn("dev_cat=prod_cat", pairs)
        self.assertEqual(len(pairs), 3)

    def test_quoted_values_unquoted(self):
        content = 'spaces:\n  - key: "a"\n    dev_id: \'xyz\'\n'
        path = write_temp(content)
        with mock.patch.dict(sys.modules, {"yaml": None}):
            spaces = read_config.parse_spaces(path)
        os.unlink(path)
        self.assertEqual(spaces[0]["key"], "a")
        self.assertEqual(spaces[0]["dev_id"], "xyz")


class TestParameterize(unittest.TestCase):
    GENERATED = """\
resources:
  genie_spaces:
    sales_genie:
      title: "Sales"
      warehouse_id: dev1234warehouse
      file_path: ../src/sales_genie.geniespace.json
      parent_path: /Workspace/Users/a@b.com/genie
"""

    def test_replaces_both_fields(self):
        out, changed = parameterize.parameterize(self.GENERATED)
        self.assertIn('warehouse_id: "${var.warehouse_id}"', out)
        self.assertIn('parent_path: "${var.parent_path}"', out)
        self.assertEqual(set(changed), {"warehouse_id", "parent_path"})

    def test_preserves_other_fields(self):
        out, _ = parameterize.parameterize(self.GENERATED)
        self.assertIn('title: "Sales"', out)
        self.assertIn("file_path: ../src/sales_genie.geniespace.json", out)

    def test_preserves_indentation(self):
        out, _ = parameterize.parameterize(self.GENERATED)
        self.assertIn('      warehouse_id: "${var.warehouse_id}"', out)

    def test_idempotent(self):
        once, _ = parameterize.parameterize(self.GENERATED)
        twice, changed = parameterize.parameterize(once)
        self.assertEqual(once, twice)
        self.assertEqual(changed, [])

    def test_missing_parent_path_is_fine(self):
        content = (
            "resources:\n  genie_spaces:\n    k:\n"
            "      warehouse_id: abc\n"
        )
        out, changed = parameterize.parameterize(content)
        self.assertIn('warehouse_id: "${var.warehouse_id}"', out)
        self.assertEqual(changed, ["warehouse_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
