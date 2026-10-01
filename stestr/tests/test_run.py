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
from unittest import mock

from stestr.commands import run
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
        ):
            with self.subTest(**kwargs):
                stdout = io.StringIO()
                ret = run.run_command(dynamic=True, stdout=stdout, **kwargs)
                self.assertEqual(2, ret)
                self.assertIn("cannot be combined", stdout.getvalue())
