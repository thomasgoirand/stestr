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
import multiprocessing
import os
import queue
import subprocess
from unittest import mock

import fixtures
import subunit
import testtools

from stestr import test_processor
from stestr.tests import base


class TestTestProcessorFixture(base.TestCase):
    def setUp(self):
        super().setUp()
        self._fixture = test_processor.TestProcessorFixture(
            mock.sentinel.test_ids,
            mock.sentinel.options,
            mock.sentinel.cmd_template,
            mock.sentinel.listopt,
            mock.sentinel.idoption,
            mock.sentinel.repository,
        )

    @mock.patch.object(subprocess, "Popen")
    @mock.patch.object(test_processor, "sys")
    def _check_start_process(
        self, mock_sys, mock_Popen, platform="win32", expected_fn=None
    ):
        mock_sys.platform = platform

        self._fixture._start_process(mock.sentinel.cmd)

        mock_Popen.assert_called_once_with(
            mock.sentinel.cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            preexec_fn=expected_fn,
        )

    def test_start_process_win32(self):
        self._check_start_process()

    def test_start_process_linux(self):
        self._check_start_process(
            platform="linux2", expected_fn=self._fixture._clear_SIGPIPE
        )


def _parse_subunit_output(raw):
    case = subunit.ByteStreamToStreamResult(io.BytesIO(raw))
    tests = []

    def _add_test(test):
        tests.append(test)

    outcomes = testtools.StreamToDict(_add_test)
    result = testtools.CopyStreamResult([testtools.StreamResult(), outcomes])
    result.startTestRun()
    try:
        case.run(result)
    finally:
        result.stopTestRun()
    return tests


def _read_connection_output(read_conn):
    chunks = []
    while True:
        try:
            chunk = os.read(read_conn.fileno(), 65536)
        except OSError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


class TestDynamicWorker(base.TestCase):
    # A stable, importable test id from this module used to exercise the
    # worker without spawning processes.
    _test_id = (
        "stestr.tests.test_test_processor"
        ".TestTestProcessorFixture.test_start_process_linux"
    )

    def _run_worker(self, job_queue):
        ctx = multiprocessing.get_context("spawn")
        read_conn, write_conn = ctx.Pipe(False)
        test_processor._dynamic_worker(job_queue, write_conn)
        write_conn.close()
        return _read_connection_output(read_conn)

    def test_dynamic_worker_exits_on_sentinel(self):
        job_queue = queue.Queue()
        job_queue.put(None)
        out = self._run_worker(job_queue)
        self.assertEqual(b"", out)

    def test_dynamic_worker_runs_queued_tests(self):
        job_queue = queue.Queue()
        job_queue.put([self._test_id])
        job_queue.put(None)
        out = self._run_worker(job_queue)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertIn(self._test_id, statuses)
        self.assertEqual("success", statuses[self._test_id])

    def test_dynamic_worker_sentinel_after_tests(self):
        # A worker must exit cleanly when it receives the sentinel, even if
        # other workers already consumed part of the queue.
        job_queue = queue.Queue()
        for _ in range(3):
            job_queue.put(None)
        out = self._run_worker(job_queue)
        self.assertEqual(b"", out)


class TestProcessorDynamicRun(base.TestCase):
    _test_id = (
        "stestr.tests.test_test_processor"
        ".TestTestProcessorFixture.test_start_process_linux"
    )

    def _get_dynamic_fixture(self, test_ids, concurrency=2):
        fixture = test_processor.TestProcessorFixture(
            test_ids,
            "python -m stestr.subunit_runner.run $IDOPTION",
            "--list",
            "--load-list $IDFILE",
            None,
            concurrency=concurrency,
            dynamic=True,
        )
        return self.useFixture(fixture)

    def test_dynamic_run_tests_returns_worker_dicts(self):
        fixture = self._get_dynamic_fixture([self._test_id])
        workers = fixture.run_tests()
        self.assertEqual(2, len(workers))
        outputs = []
        for worker in workers:
            self.assertIn("stream", worker)
            self.assertIn("proc", worker)
            worker["proc"].join()
            self.assertEqual(0, worker["proc"].exitcode)
            with os.fdopen(worker["stream"], "rb") as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertIn(self._test_id, statuses)
        self.assertEqual("success", statuses[self._test_id])

    def test_dynamic_run_tests_no_tests(self):
        fixture = self._get_dynamic_fixture([])
        workers = fixture.run_tests()
        self.assertEqual([], workers)


class TestDynamicDiscoverySeeding(base.TestCase):
    def test_worker_discovers_test_tree_before_running(self):
        # The dynamic workers must import the whole test tree the same way
        # the non-dynamic runner subprocesses do, because projects rely on
        # the import side effects of the test modules (e.g. test models
        # registering into a shared sqlalchemy metadata at import time).
        top_dir = self.useFixture(fixtures.TempDir()).path
        side_effect = os.path.join(top_dir, "test_side_effect.py")
        with open(side_effect, "w") as f:
            f.write(
                "import os\n"
                "with open(os.path.join(os.path.dirname(__file__), "
                "'marker'), 'w') as marker:\n"
                "    marker.write('discovered')\n"
            )
        user_test = os.path.join(top_dir, "test_user.py")
        with open(user_test, "w") as f:
            f.write(
                "import os\n"
                "import unittest\n"
                "\n"
                "\n"
                "class TestDiscoverySeeding(unittest.TestCase):\n"
                "    def test_side_effect_ran(self):\n"
                "        marker = os.path.join(\n"
                "            os.path.dirname(__file__), 'marker')\n"
                "        self.assertTrue(os.path.exists(marker))\n"
            )
        fixture = test_processor.TestProcessorFixture(
            ["test_user.TestDiscoverySeeding.test_side_effect_ran"],
            "python -m stestr.subunit_runner.run $IDOPTION",
            "--list",
            "--load-list $IDFILE",
            None,
            concurrency=2,
            dynamic=True,
            test_path=top_dir,
            top_dir=top_dir,
        )
        self.useFixture(fixture)
        workers = fixture.run_tests()
        outputs = []
        for worker in workers:
            worker["proc"].join()
            self.assertEqual(0, worker["proc"].exitcode)
            with os.fdopen(worker["stream"], "rb") as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual(
            "success",
            statuses["test_user.TestDiscoverySeeding.test_side_effect_ran"],
        )
