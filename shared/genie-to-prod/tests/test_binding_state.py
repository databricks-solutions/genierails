#!/usr/bin/env python3
"""Tests for Genie space binding-state detection.

Run: python3 tests/test_binding_state.py
"""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import binding_state  # noqa: E402


SUMMARY_WITH_BOUND = {
    "resources": {
        "genie_spaces": {
            "sales_genie": {"id": "01fPROD111", "title": "Sales"},
            "finance_genie": {"id": "", "title": "Finance"},
        }
    }
}


class TestGenieSpaceIds(unittest.TestCase):
    def test_extracts_ids(self):
        ids = binding_state.genie_space_ids(SUMMARY_WITH_BOUND)
        self.assertEqual(ids["sales_genie"], "01fPROD111")
        self.assertEqual(ids["finance_genie"], "")

    def test_no_summary(self):
        self.assertEqual(binding_state.genie_space_ids(None), {})

    def test_no_genie_spaces_section(self):
        self.assertEqual(binding_state.genie_space_ids({"resources": {}}), {})

    def test_no_resources_section(self):
        self.assertEqual(binding_state.genie_space_ids({}), {})

    def test_null_genie_spaces(self):
        """A target that declares no Genie spaces serializes the key as null."""
        summary = {"resources": {"genie_spaces": None}}
        self.assertEqual(binding_state.genie_space_ids(summary), {})

    def test_non_dict_entry_treated_as_unbound(self):
        summary = {"resources": {"genie_spaces": {"k": "unexpected"}}}
        self.assertEqual(binding_state.genie_space_ids(summary), {"k": ""})

    def test_missing_id_field(self):
        summary = {"resources": {"genie_spaces": {"k": {"title": "T"}}}}
        self.assertEqual(binding_state.genie_space_ids(summary), {"k": ""})


class TestClassify(unittest.TestCase):
    def setUp(self):
        self.ids = binding_state.genie_space_ids(SUMMARY_WITH_BOUND)

    def test_bound(self):
        self.assertEqual(binding_state.classify("sales_genie", self.ids),
                         binding_state.BOUND)

    def test_unbound_when_id_empty(self):
        self.assertEqual(binding_state.classify("finance_genie", self.ids),
                         binding_state.UNBOUND)

    def test_absent_when_key_unknown(self):
        self.assertEqual(binding_state.classify("nope", self.ids),
                         binding_state.ABSENT)


class TestGetState(unittest.TestCase):
    def test_state_from_summary(self):
        with mock.patch.object(binding_state, "_run_summary",
                               return_value=SUMMARY_WITH_BOUND):
            state = binding_state.get_state("prod", "prodws")
        self.assertEqual(state["sales_genie"], ("bound", "01fPROD111"))
        self.assertEqual(state["finance_genie"], ("unbound", ""))

    def test_never_deployed_yields_empty(self):
        """A bundle never deployed to prod has no state; that is not an error."""
        with mock.patch.object(binding_state, "_run_summary", return_value=None):
            self.assertEqual(binding_state.get_state("prod", "prodws"), {})


class TestRunSummary(unittest.TestCase):
    def _fake_run(self, returncode=0, stdout="{}"):
        result = mock.Mock()
        result.returncode = returncode
        result.stdout = stdout
        return mock.patch.object(binding_state.subprocess, "run",
                                 return_value=result)

    def test_force_pull_included(self):
        """--force-pull is what makes a promotion from another machine visible."""
        result = mock.Mock(returncode=0, stdout="{}")
        with mock.patch.object(binding_state.subprocess, "run",
                               return_value=result) as run:
            binding_state._run_summary("prod", "prodws")
        self.assertIn("--force-pull", run.call_args[0][0])
        self.assertIn("-o", run.call_args[0][0])
        self.assertIn("json", run.call_args[0][0])

    def test_nonzero_exit_returns_none(self):
        with self._fake_run(returncode=1, stdout=""):
            self.assertIsNone(binding_state._run_summary("prod", "prodws"))

    def test_invalid_json_returns_none(self):
        with self._fake_run(returncode=0, stdout="not json"):
            self.assertIsNone(binding_state._run_summary("prod", "prodws"))

    def test_empty_stdout_returns_none(self):
        with self._fake_run(returncode=0, stdout="   "):
            self.assertIsNone(binding_state._run_summary("prod", "prodws"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
