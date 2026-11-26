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
import signal
import subprocess
import sys
import threading
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


def _make_fake_testcase():
    """Build a no-op test case class at call time.

    The class is deliberately created inside this function, so that it
    is never a module attribute: the discovery of the real suite must
    not pick it up and run its no-op test.
    """

    class _FakeTestCase(unittest.TestCase):
        def test_fake(self):
            pass

    # unittest builds the test id from the qualified name: keep the same
    # id as the module-level class this factory replaces had.
    _FakeTestCase.__qualname__ = "_FakeTestCase"
    return _FakeTestCase


class TestDebugStreamResult(base.TestCase):
    _test_id = "stestr.tests.test_test_processor._FakeTestCase.test_fake"

    def test_start_and_done_logged(self):
        debug_stream = io.StringIO()
        subunit_stream = io.BytesIO()
        runner = test_processor._DebugSubunitTestRunner(
            stdout=subunit_stream,
            debug_stream=debug_stream,
            worker_name="worker-0",
        )
        runner.run(unittest.TestSuite([_make_fake_testcase()("test_fake")]))
        self.assertIn(
            "[stestr-dbg] worker-0 START %s\n" % self._test_id,
            debug_stream.getvalue(),
        )
        self.assertIn(
            "[stestr-dbg] worker-0 DONE %s (" % self._test_id,
            debug_stream.getvalue(),
        )
        # The subunit stream must keep carrying the result of the tests.
        tests = _parse_subunit_output(subunit_stream.getvalue())
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[self._test_id])


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
class TestDynamicWorkerDebug(base.TestCase):
    # A stable, importable test id from this module: the worker loads
    # the tests by name, so the id must resolve in the worker process.
    _test_id = (
        "stestr.tests.test_test_processor"
        ".TestTestProcessorFixture.test_start_process_linux"
    )

    def _make_pipe(self):
        ctx = multiprocessing.get_context("spawn")
        return ctx.Pipe(False)

    def test_worker_logs_progress_and_counts_groups(self):
        read_conn, write_conn = self._make_pipe()
        debug_stream = io.StringIO()
        groups_done = multiprocessing.Value("i", 0)
        job_queue = queue.Queue()
        job_queue.put([self._test_id])
        job_queue.put(None)
        with mock.patch.object(test_processor.faulthandler, "register"):
            test_processor._dynamic_worker(
                job_queue,
                write_conn,
                debug=True,
                debug_stream=debug_stream,
                groups_done=groups_done,
            )
        write_conn.close()
        out = _read_connection_output(read_conn)
        debug_out = debug_stream.getvalue()
        self.assertEqual(1, groups_done.value)
        self.assertIn("picked group #1 (1 tests)", debug_out)
        self.assertIn("START %s\n" % self._test_id, debug_out)
        self.assertIn("DONE %s (" % self._test_id, debug_out)
        # The result of the test still goes to the subunit pipe.
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[self._test_id])

    def test_filtered_out_group_counted_as_done(self):
        top_dir = self.useFixture(fixtures.TempDir()).path
        # Discovery imports the fixture module into sys.modules; pop it
        # so that a same-named module imported later from another
        # directory cannot break unittest discovery.
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
        read_conn, write_conn = self._make_pipe()
        debug_stream = io.StringIO()
        groups_done = multiprocessing.Value("i", 0)
        job_queue = queue.Queue()
        job_queue.put(["test_user.TestDiscoverySeeding.test_missing"])
        job_queue.put(None)
        with mock.patch.object(test_processor.faulthandler, "register"):
            test_processor._dynamic_worker(
                job_queue,
                write_conn,
                test_path=top_dir,
                top_dir=top_dir,
                debug=True,
                debug_stream=debug_stream,
                groups_done=groups_done,
            )
        write_conn.close()
        out = _read_connection_output(read_conn)
        debug_out = debug_stream.getvalue()
        # A group whose tests were all filtered out must still be counted
        # as done, so that the accounting of the parent stays right.
        self.assertEqual(1, groups_done.value)
        self.assertIn("picked group #1 (1 tests)", debug_out)
        self.assertNotIn("START", debug_out)
        # The missing id is reported as a failure on the subunit stream
        # instead of being silently dropped.
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual(
            "fail", statuses["test_user.TestDiscoverySeeding.test_missing"]
        )

    def test_filtered_out_group_counts_tests_done(self):
        # The test ids of a group filtered out entirely still count the
        # group as done for the debug watchdog counter, like the groups
        # that ran tests do.
        top_dir = self.useFixture(fixtures.TempDir()).path
        # Discovery imports the fixture module into sys.modules; pop it
        # so that a same-named module imported later from another
        # directory cannot break unittest discovery.
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
        read_conn, write_conn = self._make_pipe()
        debug_stream = io.StringIO()
        groups_done = multiprocessing.Value("i", 0)
        job_queue = queue.Queue()
        job_queue.put(["test_user.TestDiscoverySeeding.test_missing"])
        job_queue.put(None)
        with mock.patch.object(test_processor.faulthandler, "register"):
            test_processor._dynamic_worker(
                job_queue,
                write_conn,
                test_path=top_dir,
                top_dir=top_dir,
                debug=True,
                debug_stream=debug_stream,
                groups_done=groups_done,
            )
        write_conn.close()
        self.assertEqual(1, groups_done.value)


class _FakeWorkerProc:
    def __init__(self, name, pid, exitcode=None, on_alive=None, alive=True):
        self.name = name
        self.pid = pid
        self.exitcode = exitcode
        self.on_alive = on_alive
        self.alive = alive
        self._alive_calls = 0

    def is_alive(self):
        self._alive_calls += 1
        if self.on_alive:
            self.on_alive(self._alive_calls)
        return self.alive


class _FakeCounter:
    def __init__(self, value=0):
        self.value = value


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
class TestDynamicDebugWatchdog(base.TestCase):
    def test_watchdog_dumps_stacks_after_two_stalled_ticks(self):
        debug_stream = io.StringIO()
        counter = _FakeCounter(0)
        proc = _FakeWorkerProc("worker-0", 4242)

        def kill_worker(pid, signum):
            # The dump unblocks the worker: it exits right after.
            proc.alive = False

        with (
            mock.patch("os.kill", side_effect=kill_worker) as kill,
            mock.patch.object(test_processor.time, "sleep") as sleep,
        ):
            test_processor._dynamic_debug_watchdog(
                [{"proc": proc}],
                counter,
                1,
                interval=0.01,
                debug_stream=debug_stream,
            )
        out = debug_stream.getvalue()
        self.assertIn("groups done 0/1, pending 1, workers alive 1/1", out)
        self.assertIn("no group finished in", out)
        # The worker is named in the header written before its dump is
        # requested.
        self.assertIn("dumping stacks of worker-0", out)
        kill.assert_called_once_with(4242, signal.SIGUSR1)
        # The delay only separates two dumps: none is needed after the
        # last (and only) worker.
        dump_delays = [
            call
            for call in sleep.call_args_list
            if call.args[0] == test_processor._DEBUG_WATCHDOG_DUMP_DELAY
        ]
        self.assertEqual([], dump_delays)

    def test_watchdog_dumps_stacks_once_per_stall(self):
        # A stalled run must not have its workers re-signaled (and their
        # stacks re-dumped) on every tick of the same stall: each worker
        # is signaled at most once per stall, and again on the next
        # stall report if the run is still stuck.
        debug_stream = io.StringIO()
        counter = _FakeCounter(0)
        proc = _FakeWorkerProc("worker-0", 4242)
        # The worker never exits and never finishes a group: the
        # watchdog would loop forever, so stop it after a few stalled
        # ticks.
        stop = threading.Event()

        def stop_after_a_few_ticks(alive_calls):
            if alive_calls >= 6:
                stop.set()

        proc.on_alive = stop_after_a_few_ticks
        with (
            mock.patch("os.kill") as kill,
            mock.patch.object(test_processor.time, "sleep"),
        ):
            test_processor._dynamic_debug_watchdog(
                [{"proc": proc}],
                counter,
                1,
                interval=0.01,
                debug_stream=debug_stream,
                stop_event=stop,
            )
        out = debug_stream.getvalue()
        # One dump for the first stall, one more when the next stall
        # report comes: the worker is not re-signaled within a stall.
        self.assertEqual(1, out.count("dumping stacks of worker-0"))
        kill.assert_called_once_with(4242, signal.SIGUSR1)

    def test_watchdog_signals_workers_one_at_a_time(self):
        # Every worker dumps its stack into the same debug stream: the
        # workers are signaled one at a time, each under a header line
        # naming it, and the next worker is only signaled once the
        # previous dump had the time to land.
        debug_stream = io.StringIO()
        counter = _FakeCounter(0)
        procs = [
            _FakeWorkerProc("worker-0", 4242),
            _FakeWorkerProc("worker-1", 4243),
            _FakeWorkerProc("worker-2", 4244),
        ]
        by_pid = {proc.pid: proc for proc in procs}

        def kill_worker(pid, signum):
            # The dump unblocks the worker: it exits right after.
            by_pid[pid].alive = False

        with (
            mock.patch("os.kill", side_effect=kill_worker) as kill,
            mock.patch.object(test_processor.time, "sleep") as sleep,
        ):
            test_processor._dynamic_debug_watchdog(
                [{"proc": proc} for proc in procs],
                counter,
                1,
                interval=0.01,
                debug_stream=debug_stream,
            )
        out = debug_stream.getvalue()
        self.assertEqual(3, out.count("dumping stacks of"))
        self.assertLess(
            out.index("dumping stacks of worker-0"),
            out.index("dumping stacks of worker-1"),
        )
        self.assertLess(
            out.index("dumping stacks of worker-1"),
            out.index("dumping stacks of worker-2"),
        )
        # One signal per worker, in the order of the headers.
        self.assertEqual(
            [
                mock.call(4242, signal.SIGUSR1),
                mock.call(4243, signal.SIGUSR1),
                mock.call(4244, signal.SIGUSR1),
            ],
            kill.call_args_list,
        )
        # One inter-dump delay between two consecutive signals.
        dump_delays = [
            call
            for call in sleep.call_args_list
            if call.args[0] == test_processor._DEBUG_WATCHDOG_DUMP_DELAY
        ]
        self.assertEqual(2, len(dump_delays))

    def test_watchdog_resets_stall_when_groups_finish(self):
        debug_stream = io.StringIO()
        counter = _FakeCounter(0)

        # The counter is bumped when the second tick starts, as if a
        # group of tests finished in between.
        def bump_on_second_call(alive_calls):
            if alive_calls == 2:
                counter.value = 1

        proc = _FakeWorkerProc("worker-0", 4242, on_alive=bump_on_second_call)

        def kill_worker(pid, signum):
            # The dump unblocks the worker: it exits right after.
            proc.alive = False

        with mock.patch("os.kill", side_effect=kill_worker) as kill:
            test_processor._dynamic_debug_watchdog(
                [{"proc": proc}],
                counter,
                2,
                interval=0.01,
                debug_stream=debug_stream,
            )
        out = debug_stream.getvalue()
        self.assertIn("groups done 0/2", out)
        self.assertIn("groups done 1/2", out)
        self.assertEqual(1, out.count("no group finished in"))
        kill.assert_called_once_with(4242, signal.SIGUSR1)

    def test_watchdog_stops_when_all_workers_exit(self):
        debug_stream = io.StringIO()
        counter = _FakeCounter(0)
        proc = _FakeWorkerProc("worker-0", 4242, exitcode=1, alive=False)
        with mock.patch("os.kill") as kill:
            test_processor._dynamic_debug_watchdog(
                [{"proc": proc}],
                counter,
                1,
                interval=0.01,
                debug_stream=debug_stream,
            )
        out = debug_stream.getvalue()
        self.assertIn("exited workers: worker-0 (exitcode 1)", out)
        self.assertIn("all workers exited", out)
        kill.assert_not_called()


@unittest.skipIf(sys.platform == "win32", "dynamic mode is not supported")
class TestDynamicWorker(base.TestCase):
    # A stable, importable test id from this module used to exercise the
    # worker without spawning processes.
    _test_id = (
        "stestr.tests.test_test_processor"
        ".TestTestProcessorFixture.test_start_process_linux"
    )

    def _run_worker(self, job_queue, test_path=None, top_dir=None):
        ctx = multiprocessing.get_context("spawn")
        read_conn, write_conn = ctx.Pipe(False)
        test_processor._dynamic_worker(
            job_queue, write_conn, test_path=test_path, top_dir=top_dir
        )
        write_conn.close()
        return _read_connection_output(read_conn)

    def _write_leaky_module(self):
        """Write a temp module with tests that leave sys.stdout replaced.

        The module is written to a temp directory instead of this one, so
        that the real suite does not discover and run tests that
        deliberately corrupt sys.stdout: a mock left in place of
        sys.stdout would swallow the output of every test running after
        it in the same worker.

        Returns the top dir and the ids of the leaky tests and of a
        normal test of the module.
        """
        top_dir = self.useFixture(fixtures.TempDir()).path
        # The worker runs in this process (see _run_worker): the module
        # imported by its discovery must be dropped from sys.modules, or
        # a second temp module with the same name would fail to import.
        self.addCleanup(sys.modules.pop, "test_leaky", None)
        leaky_test = os.path.join(top_dir, "test_leaky.py")
        with open(leaky_test, "w") as f:
            f.write(
                "import io\n"
                "import sys\n"
                "import unittest\n"
                "from unittest import mock\n"
                "\n"
                "\n"
                "class TestLeakyStdout(unittest.TestCase):\n"
                "    def test_leak_stringio(self):\n"
                "        sys.stdout = io.StringIO()\n"
                "\n"
                "    def test_leak_mock(self):\n"
                "        sys.stdout = mock.Mock()\n"
                "\n"
                "    def test_ok(self):\n"
                "        pass\n"
            )
        return top_dir, (
            "test_leaky.TestLeakyStdout.test_leak_stringio",
            "test_leaky.TestLeakyStdout.test_leak_mock",
            "test_leaky.TestLeakyStdout.test_ok",
        )

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
        top_dir, (leaky_id, _, ok_id) = self._write_leaky_module()
        job_queue = queue.Queue()
        job_queue.put([leaky_id])
        job_queue.put([ok_id])
        job_queue.put(None)
        out = self._run_worker(job_queue, test_path=top_dir, top_dir=top_dir)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[leaky_id])
        self.assertEqual("success", statuses[ok_id])

    def test_worker_results_survive_mocked_stdout(self):
        # A mock left in place of sys.stdout must not silently swallow
        # the results of the next groups: the worker exits 0 either way,
        # but the tests must still be reported on the subunit stream.
        top_dir, (_, leaky_id, ok_id) = self._write_leaky_module()
        job_queue = queue.Queue()
        job_queue.put([leaky_id])
        job_queue.put([ok_id])
        job_queue.put(None)
        out = self._run_worker(job_queue, test_path=top_dir, top_dir=top_dir)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[leaky_id])
        self.assertEqual("success", statuses[ok_id])

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

    def test_worker_skips_missing_ids_from_user_list(self):
        # When the id list was supplied by the user (--failing or
        # --load-list) rather than produced by the listing of the
        # project, an id that the discovery does not find is expected
        # (the test may have been renamed since the list was written)
        # and is skipped instead of reported as a failure.
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
            report_missing_ids=False,
        )
        write_conn.close()
        out = _read_connection_output(read_conn)
        tests = _parse_subunit_output(out)
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertNotIn("test_user.TestDiscoverySeeding.test_missing", statuses)
        self.assertEqual(
            "success", statuses["test_user.TestDiscoverySeeding.test_here"]
        )

    def test_worker_reports_import_errors_once_per_run(self):
        # The workers of a run all discover the same tree: the discovery
        # import errors are reported by the first worker that hits them
        # only, so that the merged output of the parent does not repeat
        # the same synthetic failure once per worker.
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
        errors_reported = multiprocessing.Value("b", 0)
        outs = []
        for _ in range(2):
            read_conn, write_conn = multiprocessing.get_context("spawn").Pipe(False)
            job_queue = queue.Queue()
            job_queue.put(["test_user.TestDiscoverySeeding.test_here"])
            job_queue.put(None)
            test_processor._dynamic_worker(
                job_queue,
                write_conn,
                test_path=top_dir,
                top_dir=top_dir,
                errors_reported=errors_reported,
            )
            write_conn.close()
            outs.append(_read_connection_output(read_conn))
        self.assertGreater(outs[0].count(b"stestr-worker-import-errors"), 0)
        self.assertEqual(0, outs[1].count(b"stestr-worker-import-errors"))

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

        def put(item, *args, **kwargs):
            self._log.append(("put", item))
            return real_put(item, *args, **kwargs)

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

    def _get_dynamic_fixture(
        self,
        test_ids,
        concurrency=2,
        dynamic_debug=False,
        dynamic_progress=False,
    ):
        fixture = test_processor.TestProcessorFixture(
            test_ids,
            "python -m stestr.subunit_runner.run $IDOPTION",
            "--list",
            "--load-list $IDFILE",
            None,
            concurrency=concurrency,
            dynamic=True,
            dynamic_debug=dynamic_debug,
            dynamic_progress=dynamic_progress,
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
            with worker["stream"] as stream:
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
            with worker["stream"] as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses[self._test_id])

    def _capture_workers_stderr(self, fixture):
        """Run the workers of a fixture with stderr redirected to a file.

        Returns the path of the file with the stderr of the workers, so
        that their progress output can be asserted without polluting the
        output of the stestr suite itself.
        """
        tmpdir = self.useFixture(fixtures.TempDir()).path
        stderr_path = os.path.join(tmpdir, "workers_stderr")
        with open(stderr_path, "wb") as stderr_file:
            stderr_fd = os.dup(2)
            os.dup2(stderr_file.fileno(), 2)
            try:
                workers = fixture.run_tests()
                for worker in workers:
                    worker["proc"].join()
                    self.assertEqual(0, worker["proc"].exitcode)
            finally:
                os.dup2(stderr_fd, 2)
                os.close(stderr_fd)
        return stderr_path

    def test_dynamic_run_progress_disabled_by_default(self):
        # The progress display is opt-in: without it the workers are
        # silent on stderr (a run without a group regex would otherwise
        # print one line per test).
        fixture = self._get_dynamic_fixture([self._test_id])
        stderr_path = self._capture_workers_stderr(fixture)
        with open(stderr_path) as stderr_file:
            self.assertEqual("", stderr_file.read())

    def test_dynamic_run_exposes_total_tests(self):
        # The total number of tests of the run is exposed for the
        # parent, which needs it to render the results of the run.
        fixture = self._get_dynamic_fixture([self._test_id])
        fixture.run_tests()
        self.assertEqual(1, fixture._total_tests)

    def test_dynamic_run_workers_stay_silent(self):
        # The results of the workers only go to their subunit stream:
        # whatever the run options are, the workers stay silent on
        # stderr, so that their output cannot interleave with the
        # rendering of the parent.
        fixture = self._get_dynamic_fixture([self._test_id])
        stderr_path = self._capture_workers_stderr(fixture)
        with open(stderr_path) as stderr_file:
            self.assertEqual("", stderr_file.read())

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
        # A seed the user set explicitly is not overridden, and is
        # restored after the fixture is torn down.
        with mock.patch.dict(os.environ):
            os.environ["PYTHONHASHSEED"] = "5"
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
                self.assertEqual("5", os.environ["PYTHONHASHSEED"])
            self.assertEqual("5", os.environ["PYTHONHASHSEED"])

    def test_dynamic_run_restores_hash_seed(self):
        # The seed set for the run is removed again when the fixture is
        # torn down: the environment of the process must not be mutated
        # permanently.
        with mock.patch.dict(os.environ):
            os.environ.pop("PYTHONHASHSEED", None)
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
                self.assertEqual("0", os.environ["PYTHONHASHSEED"])
            self.assertNotIn("PYTHONHASHSEED", os.environ)

    def test_dynamic_run_overrides_random_hash_seed(self):
        # "random" makes every child process pick its own seed, which
        # makes the generated test ids diverge between the workers: it
        # is treated as unset. The user value is restored afterwards.
        with mock.patch.dict(os.environ):
            os.environ["PYTHONHASHSEED"] = "random"
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
                self.assertEqual("0", os.environ["PYTHONHASHSEED"])
            self.assertEqual("random", os.environ["PYTHONHASHSEED"])

    def test_dynamic_run_overrides_empty_hash_seed(self):
        # The empty value has the same recreate-per-process behavior as
        # "random": it is treated as unset too.
        with mock.patch.dict(os.environ):
            os.environ["PYTHONHASHSEED"] = ""
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
                self.assertEqual("0", os.environ["PYTHONHASHSEED"])
            self.assertEqual("", os.environ["PYTHONHASHSEED"])

    def test_dynamic_serial_fallback_does_not_touch_hash_seed(self):
        # A dynamic run that degenerates to the classic serial runner
        # (concurrency 1) spawns no worker: it must not touch the
        # environment either.
        with mock.patch.dict(os.environ):
            os.environ.pop("PYTHONHASHSEED", None)
            fixture = test_processor.TestProcessorFixture(
                [self._test_id],
                "python -m stestr.subunit_runner.run $IDOPTION",
                "--list",
                "--load-list $IDFILE",
                None,
                concurrency=1,
                dynamic=True,
            )
            with fixture:
                self.assertNotIn("PYTHONHASHSEED", os.environ)
            self.assertNotIn("PYTHONHASHSEED", os.environ)

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
            with worker["stream"] as stream:
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
            # Shrink the join timeout of the finalizer, so that the
            # terminate-then-kill ladder of the stuck worker does not
            # slow the test down.
            fixture._dynamic_join_timeout = 0.1
        sleeper.join(timeout=5)
        self.assertIsNotNone(sleeper.exitcode)

    def test_dynamic_terminate_kills_stuck_worker(self):
        # A worker that does not exit on its own (e.g. one blocked
        # writing to a pipe nobody reads) is terminated, then killed if
        # terminating was not enough.
        fixture = self._get_dynamic_fixture([self._test_id])
        proc = mock.Mock()
        # is_alive() is checked after each join: still alive after the
        # plain join, still alive after terminate, gone after kill.
        proc.is_alive.side_effect = [True, True, False]
        fixture._terminate_dynamic_workers([{"proc": proc, "stream": None}])
        self.assertEqual(
            [mock.call(timeout=fixture._dynamic_join_timeout)] * 3,
            proc.join.call_args_list,
        )
        proc.terminate.assert_called_once_with()
        proc.kill.assert_called_once_with()

    def test_dynamic_until_failure_finishes_previous_iteration(self):
        # --until-failure calls run_tests() repeatedly on the same
        # fixture, and the streams of an iteration are only fully
        # consumed after run_tests() returned: the workers of an
        # iteration are therefore collected when the next iteration
        # starts, and their stream fds must not leak into it.
        fixture = self._get_dynamic_fixture([self._test_id])
        first_workers = fixture.run_tests()
        second_workers = fixture.run_tests()
        for worker in first_workers:
            worker["proc"].join()
            self.assertIsNotNone(worker["proc"].exitcode)
            # The stream of the first iteration was closed by the start
            # of the second one.
            self.assertIsNone(worker["stream"])
        for worker in second_workers:
            worker["proc"].join()
            self.assertEqual(0, worker["proc"].exitcode)

    def test_dynamic_run_tests_progress_display(self):
        # A run without any matching test spawns no worker and displays
        # no progress.
        fixture = self._get_dynamic_fixture([], dynamic_progress=True)
        workers = fixture.run_tests()
        self.assertEqual([], workers)

    def test_dynamic_debug_run_tests(self):
        fixture = self._get_dynamic_fixture(
            [self._test_id], concurrency=2, dynamic_debug=True
        )
        # The debug output of the workers goes to the inherited stderr:
        # redirect it while the workers live, so that it does not
        # pollute the output of the stestr suite itself.
        stderr_fd = os.dup(2)
        with open(os.devnull, "wb") as devnull:
            os.dup2(devnull.fileno(), 2)
            try:
                workers = fixture.run_tests()
                outputs = []
                for worker in workers:
                    worker["proc"].join()
                    self.assertEqual(0, worker["proc"].exitcode)
                    with worker["stream"] as stream:
                        outputs.append(stream.read())
            finally:
                os.dup2(stderr_fd, 2)
                os.close(stderr_fd)
        self.assertTrue(
            any(
                thread.name == "stestr-dynamic-debug-watchdog"
                for thread in threading.enumerate()
            )
        )
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertIn(self._test_id, statuses)
        self.assertEqual("success", statuses[self._test_id])


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
            with worker["stream"] as stream:
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
            with worker["stream"] as stream:
                outputs.append(stream.read())
        tests = _parse_subunit_output(b"".join(outputs))
        statuses = {test["id"]: test["status"] for test in tests}
        self.assertEqual("success", statuses["test_user.TestClassFixtures.test_here"])
        self.assertEqual("success", statuses["test_user.TestClassFixtures.test_there"])
        # Both tests ran in a single group: the class fixtures ran
        # exactly once.
        with open(count_file) as f:
            self.assertEqual("updown", f.read())
