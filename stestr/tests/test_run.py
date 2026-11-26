# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import io
import unittest
from unittest import mock

from stestr.commands import run
from stestr.subunit_runner import program
from stestr.tests import base


class TestRunCommand(base.TestCase):
    def test_to_int_positive_int(self):
        self.assertEqual(29, run._to_int(29))

    def test_to_int_positive_int_str(self):
        self.assertEqual(42, run._to_int("42"))

    def test_to_int_negative_int(self):
        self.assertEqual(-2, run._to_int(-2))

    def test_to_int_negative_int_str(self):
        self.assertEqual(-45, run._to_int("-45"))

    def test_to_int_invalid_str(self):
        fake_stderr = io.StringIO()
        out = run._to_int("I am not an int", out=fake_stderr)
        expected = 'Unable to convert "I am not an int" to an integer.  ' "Using 0.\n"
        self.assertEqual(fake_stderr.getvalue(), expected)
        self.assertEqual(0, out)

    def test_to_int_none(self):
        fake_stderr = io.StringIO()
        out = run._to_int(None, out=fake_stderr)
        expected = 'Unable to convert "None" to an integer.  ' "Using 0.\n"
        self.assertEqual(fake_stderr.getvalue(), expected)
        self.assertEqual(0, out)

    def test_dynamic_refused_on_win32(self):
        stdout = io.StringIO()
        with mock.patch("sys.platform", "win32"):
            ret = run.run_command(dynamic=True, stdout=stdout)
        self.assertEqual(2, ret)
        self.assertEqual(
            "The dynamic scheduler is not supported on Windows\n",
            stdout.getvalue(),
        )

    def test_dynamic_refused_with_unsupported_options_on_win32(self):
        # The option conflict is reported even on Windows, where the
        # dynamic scheduler is refused for platform reasons anyway: a
        # caller passing an unsupported option must learn that the
        # option is the problem, not the platform.
        for kwargs in ({"pdb": "some_test"}, {"isolated": True}):
            with self.subTest(**kwargs):
                stdout = io.StringIO()
                with mock.patch("sys.platform", "win32"):
                    ret = run.run_command(dynamic=True, stdout=stdout, **kwargs)
                self.assertEqual(2, ret)
                self.assertIn("cannot be combined", stdout.getvalue())

    def test_dynamic_refused_with_unsupported_options(self):
        # The dynamic scheduler cannot honor these options: they must be
        # rejected instead of silently falling back to the static
        # scheduler.
        for kwargs in (
            {"isolated": True},
            {"analyze_isolation": True},
            {"no_discover": "some_test"},
            {"pdb": "some_test"},
            {"worker_path": "workers.yaml"},
            {"serial": True},
        ):
            with self.subTest(**kwargs):
                stdout = io.StringIO()
                ret = run.run_command(dynamic=True, stdout=stdout, **kwargs)
                self.assertEqual(2, ret)
                self.assertIn("cannot be combined", stdout.getvalue())


class _IdTestCase(unittest.TestCase):
    """A test case with a controlled id, runnable as-is."""

    def __init__(self, test_id):
        super().__init__("test_id")
        self._test_id = test_id

    def test_id(self):
        pass

    def id(self):
        return self._test_id


class TestOrderSuiteByIds(base.TestCase):
    @staticmethod
    def _suite(ids):
        return unittest.TestSuite([_IdTestCase(test_id) for test_id in ids])

    @staticmethod
    def _order(suite):
        return [test.id() for test in program.iterate_tests(suite)]

    def test_honors_the_list_order(self):
        # The runner honors the order of the id list it was given
        # (--random feeds a shuffled list): the tests are run in the
        # order of the list, not in the discovery order.
        suite = self._suite(["test_a", "test_b", "test_c"])
        ordered = program.order_suite_by_ids(suite, ["test_c", "test_a", "test_b"])
        self.assertEqual(["test_c", "test_a", "test_b"], self._order(ordered))

    def test_skips_unknown_ids(self):
        suite = self._suite(["test_a", "test_b"])
        ordered = program.order_suite_by_ids(suite, ["test_b", "test_gone", "test_a"])
        self.assertEqual(["test_b", "test_a"], self._order(ordered))

    def test_runs_a_duplicated_id_once(self):
        suite = self._suite(["test_a", "test_b"])
        ordered = program.order_suite_by_ids(suite, ["test_a", "test_a", "test_b"])
        self.assertEqual(["test_a", "test_b"], self._order(ordered))

    def test_keeps_unlisted_tests_at_the_end(self):
        # The tests the list did not mention (the discovery kept them
        # but the list was a subset) keep running at the end, in their
        # original order: no test is lost by the reordering.
        suite = self._suite(["test_a", "test_b", "test_c"])
        ordered = program.order_suite_by_ids(suite, ["test_c"])
        self.assertEqual(["test_c", "test_a", "test_b"], self._order(ordered))

    def test_orders_a_nested_suite(self):
        suite = unittest.TestSuite(
            [
                self._suite(["test_a", "test_b"]),
                self._suite(["test_c"]),
            ]
        )
        ordered = program.order_suite_by_ids(suite, ["test_b", "test_c", "test_a"])
        self.assertEqual(["test_b", "test_c", "test_a"], self._order(ordered))
