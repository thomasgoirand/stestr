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
import sys
import time
import unittest
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


class _LeakyStdoutTestCase(unittest.TestCase):
    # Test cases that leave sys.stdout replaced, like a test with a
    # broken cleanup does: the dynamic worker must survive them.

    def test_leak_stringio(self):
        sys.stdout = io.StringIO()

    def test_leak_mock(self):
        sys.stdout = mock.Mock()


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
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

    def test_worker_results_survive_leaked_stdout(self):
        # A test that leaves sys.stdout replaced must not redirect the
        # results of the next groups of the worker: the runner is bound
        # to the subunit stream wrapper, like the subprocess runner binds
        # its stream once, not to whatever sys.stdout points at.
        leaky_id = (
            "stestr.tests.test_test_processor"
            "._LeakyStdoutTestCase.test_leak_stringio"
        )
        job_queue = queue.Queue()
        job_queue.put([leaky_id])
        job_queue.put([self._test_id])
        job_queue.put(None)
        out = self._run_worker(job_queue)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[leaky_id])
        self.assertEqual("success", statuses[self._test_id])

    def test_worker_results_survive_mocked_stdout(self):
        # A mock left in place of sys.stdout must not silently swallow
        # the results of the next groups: the worker exits 0 either way,
        # but the tests must still be reported on the subunit stream.
        leaky_id = (
            "stestr.tests.test_test_processor" "._LeakyStdoutTestCase.test_leak_mock"
        )
        job_queue = queue.Queue()
        job_queue.put([leaky_id])
        job_queue.put([self._test_id])
        job_queue.put(None)
        out = self._run_worker(job_queue)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[leaky_id])
        self.assertEqual("success", statuses[self._test_id])

    def test_worker_reports_missing_ids_as_failures(self):
        # An id that the discovery of the worker does not know (e.g. a
        # module that imports under the listing interpreter but not in
        # the worker) must be reported as a failure instead of being
        # silently dropped and counted as done.
        top_dir = self.useFixture(fixtures.TempDir()).path
        self.addCleanup(sys.modules.pop, "test_user", None)
        user_test = os.path.join(top_dir, "test_user.py")
        with open(user_test, "w") as f:
            f.write(
                "import unittest\n"
                "\n"
                "\n"
                "class TestDiscoverySeeding(unittest.TestCase):\n"
                "    def test_here(self):\n"
                "        pass\n"
            )
        read_conn, write_conn = multiprocessing.get_context("spawn").Pipe(False)
        job_queue = queue.Queue()
        job_queue.put(
            [
                "test_user.TestDiscoverySeeding.test_missing",
                "test_user.TestDiscoverySeeding.test_here",
            ]
        )
        job_queue.put(None)
        test_processor._dynamic_worker(
            job_queue,
            write_conn,
            test_path=top_dir,
            top_dir=top_dir,
        )
        write_conn.close()
        out = _read_connection_output(read_conn)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual(
            "fail", statuses["test_user.TestDiscoverySeeding.test_missing"]
        )
        self.assertEqual(
            "success", statuses["test_user.TestDiscoverySeeding.test_here"]
        )

    def test_worker_reports_discovery_import_errors(self):
        # A module that fails to import at discovery time must be
        # reported like the non-dynamic runner reports it: the run fails
        # with the import errors, instead of silently dropping the tests
        # of that module.
        top_dir = self.useFixture(fixtures.TempDir()).path
        self.addCleanup(sys.modules.pop, "test_user", None)
        broken_test = os.path.join(top_dir, "test_broken.py")
        with open(broken_test, "w") as f:
            f.write("import not_a_real_module_xyz\n")
        user_test = os.path.join(top_dir, "test_user.py")
        with open(user_test, "w") as f:
            f.write(
                "import unittest\n"
                "\n"
                "\n"
                "class TestDiscoverySeeding(unittest.TestCase):\n"
                "    def test_here(self):\n"
                "        pass\n"
            )
        read_conn, write_conn = multiprocessing.get_context("spawn").Pipe(False)
        job_queue = queue.Queue()
        job_queue.put(["test_user.TestDiscoverySeeding.test_here"])
        job_queue.put(None)
        test_processor._dynamic_worker(
            job_queue,
            write_conn,
            test_path=top_dir,
            top_dir=top_dir,
        )
        write_conn.close()
        out = _read_connection_output(read_conn)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual(
            "success", statuses["test_user.TestDiscoverySeeding.test_here"]
        )
        self.assertEqual("fail", statuses["stestr-worker-import-errors"])
        self.assertIn(b"not_a_real_module_xyz", out)


class _RecordingProcess:
    """Proxy recording the start of a spawned worker process."""

    def __init__(self, real, log):
        self._real = real
        self._log = log

    def start(self):
        self._log.append(("start", self._real.name))
        self._real.start()

    def __getattr__(self, name):
        return getattr(self._real, name)


class _RecordingContext:
    """Wrap a multiprocessing context recording its lifecycle calls."""

    def __init__(self, real, log):
        self._real = real
        self._log = log

    def Queue(self):
        # The queue closures are safe: Queue pickles through an explicit
        # __getstate__, so the instance attributes are not carried over
        # to the spawned workers.
        queue = self._real.Queue()
        real_cancel = queue.cancel_join_thread
        real_put = queue.put

        def cancel_join_thread():
            self._log.append("cancel_join_thread")
            real_cancel()

        def put(item):
            self._log.append(("put", item))
            return real_put(item)

        queue.cancel_join_thread = cancel_join_thread
        queue.put = put
        return queue

    def Process(self, *args, **kwargs):
        return _RecordingProcess(self._real.Process(*args, **kwargs), self._log)

    def __getattr__(self, name):
        return getattr(self._real, name)


class _RecordingMultiprocessing:
    """Wrap the multiprocessing module recording its lifecycle calls."""

    def __init__(self, real, log):
        self._real = real
        self._log = log

    def get_context(self, name):
        return _RecordingContext(self._real.get_context(name), self._log)

    def __getattr__(self, name):
        return getattr(self._real, name)


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
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
        self.assertEqual(1, len(workers))
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

    def test_dynamic_run_spawns_one_worker_per_group(self):
        # Spawning one worker per CPU for a handful of groups would run a
        # full test tree discovery in each of them for nothing: the
        # worker count is capped by the number of groups to run.
        fixture = self._get_dynamic_fixture([self._test_id], concurrency=8)
        workers = fixture.run_tests()
        self.assertEqual(1, len(workers))
        for worker in workers:
            worker["proc"].join()
            self.assertEqual(0, worker["proc"].exitcode)
        outputs = []
        for worker in workers:
            with os.fdopen(worker["stream"], "rb") as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[self._test_id])

    def test_dynamic_run_shares_hash_seed(self):
        # The listing process and the workers must generate the same test
        # ids: ddt and similar test generators derive the ids from the
        # iteration order of sets (e.g. @ddt.data(*a_frozenset)), which
        # depends on the per-process hash seed. A dynamic run therefore
        # shares one seed with its child processes.
        with mock.patch.dict(os.environ):
            os.environ.pop("PYTHONHASHSEED", None)
            self._get_dynamic_fixture([self._test_id])
            self.assertEqual("0", os.environ["PYTHONHASHSEED"])

    def test_dynamic_run_honors_existing_hash_seed(self):
        # A seed the user set explicitly is not overridden.
        with mock.patch.dict(os.environ):
            os.environ["PYTHONHASHSEED"] = "5"
            self._get_dynamic_fixture([self._test_id])
            self.assertEqual("5", os.environ["PYTHONHASHSEED"])

    def test_classic_run_does_not_touch_hash_seed(self):
        # Only the dynamic scheduler needs the shared seed: the classic
        # runner keeps the environment untouched.
        with mock.patch.dict(os.environ):
            os.environ.pop("PYTHONHASHSEED", None)
            fixture = test_processor.TestProcessorFixture(
                [self._test_id],
                "python -m stestr.subunit_runner.run $IDOPTION",
                "--list",
                "--load-list $IDFILE",
                None,
            )
            self.useFixture(fixture)
            self.assertNotIn("PYTHONHASHSEED", os.environ)

    def test_dynamic_run_does_not_compute_static_partition(self):
        # The dynamic scheduler groups the test ids itself: computing the
        # static partition (or the worker file groups) first would be
        # discarded work, and a worker file must not silently change the
        # grouping of a dynamic run.
        fixture = self._get_dynamic_fixture([self._test_id])
        with (
            mock.patch.object(test_processor.scheduler, "partition_tests") as partition,
            mock.patch.object(
                test_processor.scheduler, "generate_worker_partitions"
            ) as generate,
        ):
            workers = fixture.run_tests()
        for worker in workers:
            worker["proc"].join()
            self.assertEqual(0, worker["proc"].exitcode)
        partition.assert_not_called()
        generate.assert_not_called()
        outputs = []
        for worker in workers:
            with os.fdopen(worker["stream"], "rb") as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[self._test_id])

    def test_dynamic_run_starts_workers_before_feeding(self):
        # The workers must exist before the groups are fed to the queue:
        # the queue's internal semaphore is bounded by SEM_VALUE_MAX
        # (32767 on macOS), so a run with more groups than that would
        # deadlock in put() with no consumer. The queue must also not
        # hold the interpreter at exit when the workers stopped
        # consuming.
        fixture = self._get_dynamic_fixture([self._test_id])
        log = []
        with mock.patch.object(
            test_processor,
            "multiprocessing",
            _RecordingMultiprocessing(multiprocessing, log),
        ):
            workers = fixture.run_tests()
        for worker in workers:
            worker["proc"].join()
        self.assertIn("cancel_join_thread", log)
        first_start = log.index(("start", "worker-0"))
        first_put = next(index for index, entry in enumerate(log) if entry[0] == "put")
        self.assertLess(first_start, first_put)

    def test_fixture_cleanup_terminates_lingering_workers(self):
        # A run interrupted before the workers consumed every group (e.g.
        # Ctrl-C, or all the workers crashed early) must not leave the
        # worker processes behind: the fixture cleanup terminates any
        # worker that is still alive.
        fixture = test_processor.TestProcessorFixture(
            [self._test_id],
            "python -m stestr.subunit_runner.run $IDOPTION",
            "--list",
            "--load-list $IDFILE",
            None,
            concurrency=2,
            dynamic=True,
        )
        with fixture:
            workers = fixture.run_tests()
            for worker in workers:
                worker["proc"].join()
            ctx = multiprocessing.get_context("spawn")
            sleeper = ctx.Process(target=time.sleep, args=(60,))
            sleeper.start()
            workers[0]["proc"] = sleeper
        sleeper.join(timeout=5)
        self.assertIsNotNone(sleeper.exitcode)

    def test_dynamic_run_tests_no_tests(self):
        fixture = self._get_dynamic_fixture([])
        workers = fixture.run_tests()
        self.assertEqual([], workers)


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
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

    def test_worker_runs_class_fixtures_once_per_group(self):
        # Each group of tests runs as a single TestSuite, so the class
        # level fixtures (setUpClass/tearDownClass) are set up once per
        # group, like in the non-dynamic mode: running the test ids as
        # bare TestCases would never execute them.
        top_dir = self.useFixture(fixtures.TempDir()).path
        count_file = os.path.join(top_dir, "setup_count")
        user_test = os.path.join(top_dir, "test_user.py")
        with open(user_test, "w") as f:
            f.write(
                "import unittest\n"
                "\n"
                "\n"
                "class TestClassFixtures(unittest.TestCase):\n"
                "    @classmethod\n"
                "    def setUpClass(cls):\n"
                "        with open(%r, 'a') as f:\n"
                "            f.write('up')\n"
                "\n"
                "    @classmethod\n"
                "    def tearDownClass(cls):\n"
                "        with open(%r, 'a') as f:\n"
                "            f.write('down')\n"
                "\n"
                "    def test_here(self):\n"
                "        pass\n"
                "\n"
                "    def test_there(self):\n"
                "        pass\n" % (count_file, count_file)
            )
        fixture = test_processor.TestProcessorFixture(
            [
                "test_user.TestClassFixtures.test_here",
                "test_user.TestClassFixtures.test_there",
            ],
            "python -m stestr.subunit_runner.run $IDOPTION",
            "--list",
            "--load-list $IDFILE",
            None,
            concurrency=2,
            dynamic=True,
            # Group the tests by class, so that both tests of the class
            # are scheduled as a single group.
            group_callback=lambda test_id: test_id.rsplit(".", 1)[0],
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
        self.assertEqual("success", statuses["test_user.TestClassFixtures.test_here"])
        self.assertEqual("success", statuses["test_user.TestClassFixtures.test_there"])
        # Both tests ran in a single group: the class fixtures ran
        # exactly once.
        with open(count_file) as f:
            self.assertEqual("updown", f.read())
