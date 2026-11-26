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

import faulthandler
import io
import multiprocessing
import os
import queue
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import fixtures
from subunit.test_results import AutoTimingTestResultDecorator
from subunit import v2
from testtools import ExtendedToStreamDecorator

from stestr import results
from stestr import scheduler
from stestr import selection
from stestr.subunit_runner import program
from stestr.subunit_runner import run
from stestr import testlist


def _emit_subunit_failure(stream, test_id, message):
    """Emit a failed subunit test on the stream of a dynamic worker.

    Used for the errors the worker cannot turn into a test run: a test id
    the discovery does not know, or the errors of the discovery itself.
    The parent consumes them from the subunit stream like any other test
    result, so that they fail the run instead of being silently dropped.
    """
    v2.StreamResultToBytes(stream).status(
        test_id=test_id,
        test_status="fail",
        file_name="traceback",
        mime_type="text/plain;charset=utf8",
        file_bytes=message.encode("utf8"),
    )


_DEBUG_WATCHDOG_INTERVAL = 30
# Seconds the watchdog waits after signaling a worker before signaling
# the next one, so that their stack dumps do not interleave in the
# shared debug stream.
_DEBUG_WATCHDOG_DUMP_DELAY = 1
# Seconds _terminate_dynamic_workers waits for a worker to exit before
# terminating (and then killing) it.
_DYNAMIC_JOIN_TIMEOUT = 5
# The subunit test statuses that mark the end of a test. See the
# status_mask of subunit.v2.StreamResultToBytes.
_FINAL_TEST_STATUSES = frozenset(["success", "fail", "skip", "xfail", "uxsuccess"])


class _DebugStreamResult:
    """StreamResult wrapper logging the per-test progress of a worker.

    It is inserted at the innermost level of the result chain built by
    _DebugSubunitTestRunner: everything is forwarded unchanged to the
    wrapped stream, and a line is written to the debug stream when a
    test starts (status in-progress) and when it reaches a final status,
    so that the state of each worker stays visible while it runs, even
    when a test never completes.
    """

    def __init__(self, result, debug_stream, worker_name):
        self._result = result
        self._debug_stream = debug_stream
        self._worker_name = worker_name
        self._start_times = {}

    def status(
        self,
        test_id=None,
        test_status=None,
        test_tags=None,
        runnable=True,
        file_name=None,
        file_bytes=None,
        eof=False,
        mime_type=None,
        route_code=None,
        timestamp=None,
    ):
        if test_status == "inprogress":
            self._start_times[test_id] = time.monotonic()
            self._write("[stestr-dbg] %s START %s\n" % (self._worker_name, test_id))
        elif test_status in _FINAL_TEST_STATUSES and test_id in self._start_times:
            duration = time.monotonic() - self._start_times.pop(test_id)
            self._write(
                "[stestr-dbg] %s DONE %s (%.2fs)\n"
                % (self._worker_name, test_id, duration)
            )
        self._result.status(
            test_id=test_id,
            test_status=test_status,
            test_tags=test_tags,
            runnable=runnable,
            file_name=file_name,
            file_bytes=file_bytes,
            eof=eof,
            mime_type=mime_type,
            route_code=route_code,
            timestamp=timestamp,
        )

    def _write(self, line):
        self._debug_stream.write(line)
        self._debug_stream.flush()

    def __getattr__(self, name):
        return getattr(self._result, name)


class _DebugSubunitTestRunner(run.SubunitTestRunner):
    """A SubunitTestRunner that logs per-test progress to a debug stream.

    Otherwise identical to SubunitTestRunner: the subunit stream of the
    tests is built exactly the same way, the debug lines are written to
    a separate stream (the worker's stderr in real runs).
    """

    def __init__(
        self,
        failfast=False,
        tb_locals=False,
        stdout=sys.stdout,
        debug_stream=None,
        worker_name=None,
    ):
        super().__init__(failfast=failfast, tb_locals=tb_locals, stdout=stdout)
        self._debug_stream = debug_stream if debug_stream is not None else sys.stderr
        self._worker_name = worker_name

    def run(self, test):
        "Run the given test case or test suite."
        result, _ = self._list(test)
        result = _DebugStreamResult(result, self._debug_stream, self._worker_name)
        result = ExtendedToStreamDecorator(result)
        result = AutoTimingTestResultDecorator(result)
        if self.failfast is not None:
            result.failfast = self.failfast
            result.tb_locals = self.tb_locals
        result.startTestRun()
        try:
            test(result)
        finally:
            result.stopTestRun()
        return result


def _dynamic_debug_watchdog(
    workers,
    groups_done,
    total_groups,
    interval=_DEBUG_WATCHDOG_INTERVAL,
    debug_stream=None,
    stop_event=None,
):
    """Report the state of a dynamic scheduler run until it ends.

    Started as a daemon thread by the dynamic scheduling mode of
    TestProcessorFixture.run_tests() when debugging is enabled. Every
    interval seconds it writes to the debug stream how many groups of
    tests were completed, how many are still pending and how many
    workers are still alive. When no group finished across two
    consecutive ticks, the still-alive workers are signaled one at a
    time with SIGUSR1 and each of them dumps its Python stack (see
    _dynamic_worker), so that a hung test can be located in the log.
    The dumps of concurrently signaled workers would interleave into
    an unreadable mix in the shared debug stream, so a worker is only
    signaled once the previous dump had the time to land, and each
    worker is signaled at most once per stall: the workers are
    signaled again by a later stall, once the run made progress in
    between and stalled again.
    The thread returns as soon as every worker exited.

    :param workers: The list of worker dicts built by run_tests(), each
        carrying the worker process under the "proc" key.
    :param groups_done: A multiprocessing.Value incremented by each
        worker after every completed group of tests.
    :param int total_groups: The number of groups scheduled for the run.
    :param int interval: Seconds between two reports.
    :param debug_stream: The stream the reports are written to, defaults
        to sys.stderr.
    :param stop_event: A threading.Event set by the fixture finalizer to
        stop the watchdog promptly once the run ended.
    """
    if debug_stream is None:
        debug_stream = sys.stderr
    last_done = -1
    stalled_ticks = 0
    last_progress_time = time.monotonic()
    # The workers already signaled during the current stall, so that a
    # stalled run does not have its workers re-signaled (and their
    # stacks re-dumped) on every tick of the same stall.
    signaled = set()
    while True:
        if stop_event is not None:
            if stop_event.wait(interval):
                return
        else:
            time.sleep(interval)
        alive = []
        dead = []
        for w in workers:
            if w["proc"].is_alive():
                alive.append(w)
            else:
                dead.append(w)
        done = groups_done.value
        debug_stream.write(
            "[stestr-dbg] watchdog: groups done %s/%s, pending %s, "
            "workers alive %s/%s\n"
            % (
                done,
                total_groups,
                total_groups - done,
                len(alive),
                len(workers),
            )
        )
        if dead:
            debug_stream.write(
                "[stestr-dbg] watchdog: exited workers: %s\n"
                % ", ".join(
                    "%s (exitcode %s)" % (w["proc"].name, w["proc"].exitcode)
                    for w in dead
                )
            )
        if not alive:
            debug_stream.write("[stestr-dbg] watchdog: all workers exited\n")
            debug_stream.flush()
            return
        if done == last_done:
            stalled_ticks += 1
            if stalled_ticks >= 2:
                debug_stream.write(
                    "[stestr-dbg] watchdog: no group finished in %ss\n"
                    % int(time.monotonic() - last_progress_time)
                )
                debug_stream.flush()
                # Signal one worker at a time: every worker dumps its
                # stack into the same debug stream, so the dumps of
                # concurrently signaled workers would interleave. The
                # header names the worker, and the next worker is only
                # signaled once the previous dump had the time to land.
                # Each worker is signaled at most once per stall: the
                # signaled set is cleared when the run makes progress,
                # so a later stall signals the workers again, but a
                # stalled run does not have its workers re-signaled (and
                # their stacks re-dumped) on every tick of the same
                # stall.
                for w in alive:
                    if not w["proc"].is_alive():
                        # A previous dump may have unblocked the worker
                        # (or made it crash): only signal the ones that
                        # are still alive.
                        continue
                    if w["proc"] in signaled:
                        continue
                    if signaled:
                        time.sleep(_DEBUG_WATCHDOG_DUMP_DELAY)
                    signaled.add(w["proc"])
                    debug_stream.write(
                        "[stestr-dbg] watchdog: dumping stacks of %s\n" % w["proc"].name
                    )
                    debug_stream.flush()
                    try:
                        os.kill(w["proc"].pid, signal.SIGUSR1)
                    except OSError:
                        pass
        else:
            stalled_ticks = 0
            last_progress_time = time.monotonic()
            signaled.clear()
        last_done = done
        debug_stream.flush()


def _dynamic_worker(
    job_queue,
    subunit_pipe,
    test_path=None,
    top_dir=None,
    debug=False,
    debug_stream=None,
    groups_done=None,
    report_missing_ids=True,
    errors_reported=None,
):
    """Run the groups of test ids from a queue until the sentinel.

    This is the target function used by the worker processes spawned for
    dynamic scheduling. Each worker pulls a single group of test ids at a
    time from the shared queue and runs it, writing the subunit output to
    the write end of a pipe that is read by the parent process. After the
    groups the parent enqueues a None sentinel per worker, and a worker
    exits as soon as it receives one. This guarantees a worker only exits
    after every group was handed to some worker, without any race on an
    empty() check between workers.

    :param job_queue: The queue with the groups of test ids to run, shared
        by all the workers of a run.
    :param subunit_pipe: The write end of the pipe used to send the
        subunit output of the tests back to the parent process.
    :param test_path: The path of the test tree. When set, the worker
        loads the whole tree with a unittest discovery first, like the
        runner subprocesses of the non-dynamic mode do, and runs the
        discovered tests matched by id. Optional.
    :param top_dir: The top level directory used by that discovery.
        Optional.
    :param bool debug: Log the progress of the worker to the debug
        stream: the groups picked from the queue and each test start
        (START) and completion (DONE), and dump the stack of the worker
        to the debug stream whenever it receives SIGUSR1 (sent by the
        watchdog of the parent process, see _dynamic_debug_watchdog).
    :param debug_stream: The stream the debug output is written to,
        defaults to sys.stderr. Only used when debug is true.
    :param groups_done: A multiprocessing.Value shared with the parent
        process, incremented after every completed group of tests.
        Optional.
    :param bool report_missing_ids: Report the test ids of a group that
        the discovery of the worker did not find as failed tests. On by
        default. When the id list was supplied by the user (--failing or
        --load-list) rather than produced by the listing of the project,
        a missing id is expected (the test may have been renamed since
        the list was written) and must not fail the run: pass False.
    :param errors_reported: A multiprocessing.Value shared with the
        other workers of the run, used as a flag so that the discovery
        import errors are reported by the first worker that hits them
        only: the workers of a run all discover the same tree, and one
        synthetic failure test per worker would repeat the same errors
        once per worker. Optional.
    """
    # NOTE(mtreinish): Duplicate the file descriptor of the pipe so the
    # writer is decoupled from the Connection object's lifetime. Without
    # this you'll be fighting random bad file descriptor errors.
    subunit_stream = os.fdopen(os.dup(subunit_pipe.fileno()), "wb")
    parent_stdout = sys.stdout
    # The non-dynamic mode loads the test ids from a subprocess started
    # with python -m which puts the current working directory on sys.path.
    # Do the same here so the tests of the project under test are
    # importable by name in the worker.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    # The non-dynamic runner subprocess is started with
    # `python -m stestr.subunit_runner.run`, so sys.argv[0] is the path of
    # the run module. Some projects derive names from it, e.g. the
    # iptables filter chains of Neutron, so present the same value here.
    sys.argv = [run.__file__]
    # Point the worker's stdout at the subunit stream (buffered text, the
    # same as what a subprocess runner gets) so that any output the tests
    # write is captured in the subunit stream instead of leaking into the
    # parent's console. The runners of the groups are bound to this
    # wrapper object, not to sys.stdout: a test that leaves sys.stdout
    # replaced must not redirect the results of the later groups of the
    # worker, like the subprocess runner binds its stream once.
    stdout_wrapper = io.TextIOWrapper(subunit_stream)
    sys.stdout = stdout_wrapper
    if debug:
        if debug_stream is None:
            debug_stream = sys.stderr
        # NOTE: The handler is registered before the discovery runs, so
        # that a handler registered later by the code of the project
        # under test (e.g. at discovery time) REPLACES this one instead
        # of being replaced by it: the stack dumps of the watchdog must
        # not steal the signal from a handler the project installed for
        # its own debugging. The worker keeps running after a dump, so
        # a slow test must not be killed by it.
        faulthandler.register(signal.SIGUSR1, file=debug_stream, all_threads=True)
        worker_name = multiprocessing.current_process().name
        picked_groups = 0
    try:
        test_map = {}
        if test_path:
            # The non-dynamic mode runs the tests of a worker from a
            # subprocess that loads the whole test tree through a unittest
            # discovery and then filters it by the test ids. Mirror it
            # here, so that a worker starts with the same loaded state as
            # a non-dynamic runner: some projects rely on those import
            # side effects, e.g. test models that register into a shared
            # sqlalchemy metadata when their module is imported, and
            # without them the tables of those models are never created
            # in the test databases. It also gives the actual test
            # objects to run, matched by id: the unittest name loader
            # cannot resolve the ids generated dynamically by ddt.
            # A fresh loader, so that the errors below are the ones of
            # this worker's discovery only.
            loader = unittest.TestLoader()
            discovered = loader.discover(test_path, top_level_dir=top_dir)
            for test in program.iterate_tests(discovered):
                test_map[test.id()] = test
            if loader.errors:
                # The discovery failed to import some modules: report the
                # errors like the non-dynamic runner reports them, so
                # that the run fails instead of silently dropping the
                # tests of those modules. Only the first worker that
                # hits them reports them: the workers of a run all
                # discover the same tree, and one synthetic failure test
                # per worker would repeat the same errors once per
                # worker.
                if errors_reported is not None:
                    with errors_reported.get_lock():
                        report_errors = errors_reported.value == 0
                        if report_errors:
                            errors_reported.value = 1
                else:
                    report_errors = True
                if report_errors:
                    _emit_subunit_failure(
                        stdout_wrapper,
                        "stestr-worker-import-errors",
                        "\n".join(loader.errors),
                    )

        while True:
            group = job_queue.get()
            if group is None:
                break
            if debug:
                picked_groups += 1
                debug_stream.write(
                    "[stestr-dbg] %s picked group #%s (%s tests)\n"
                    % (worker_name, picked_groups, len(group))
                )
                debug_stream.flush()
            if test_path:
                tests = []
                for test_id in group:
                    try:
                        tests.append(test_map[test_id])
                    except KeyError:
                        if not report_missing_ids:
                            # The id list was supplied by the user
                            # (--failing or --load-list) rather than
                            # produced by the listing of the project: a
                            # missing id is expected (the test may have
                            # been renamed since the list was written)
                            # and must not fail the run.
                            continue
                        # The id is not in the discovery of the worker:
                        # report it as a failure instead of silently
                        # dropping it and counting it as done (e.g. a
                        # module that imports under the listing
                        # interpreter but not in the worker).
                        _emit_subunit_failure(
                            stdout_wrapper,
                            test_id,
                            "The test %s was not found by the discovery "
                            "of the worker: it may not be importable by "
                            "the worker interpreter." % test_id,
                        )
                if not tests:
                    # The whole group was filtered out: count it as done
                    # anyway so that the accounting of the parent stays
                    # right.
                    if groups_done is not None:
                        with groups_done.get_lock():
                            groups_done.value += 1
                    continue
            else:
                tests = [
                    unittest.defaultTestLoader.loadTestsFromName(test_id)
                    for test_id in group
                ]
            # Run the group as a single suite, so that the class level
            # fixtures are handled by the suite like in the non-dynamic
            # mode.
            if debug:
                runner = _DebugSubunitTestRunner(
                    stdout=stdout_wrapper,
                    debug_stream=debug_stream,
                    worker_name=worker_name,
                )
            else:
                runner = run.SubunitTestRunner(stdout=stdout_wrapper)
            runner.run(unittest.TestSuite(tests))
            if groups_done is not None:
                with groups_done.get_lock():
                    groups_done.value += 1
    finally:
        # Flush the wrapper, not sys.stdout: a test may have left a
        # replaced sys.stdout behind, and it must not break the flush of
        # the results at exit.
        stdout_wrapper.flush()
        subunit_stream.close()
        sys.stdout = parent_stdout


class TestProcessorFixture(fixtures.Fixture):
    """Write a temporary file to disk with test ids in it.

    The TestProcessorFixture is used to handle the lifecycle of running
    the subunit.run commands. A fixture is used for this class to handle
    the temporary list files creation.

    :param test_ids: The test_ids to use. May be None indicating that
        no ids are known and they should be discovered by listing or
        configuration if they must be known to run tests. Test ids are
        needed to run tests when filtering or partitioning is needed: if
        the run concurrency is > 1 partitioning is needed, and filtering is
        needed if the user has passed in filters.
    :param cmd_template: string to be used for the command that will be
        filled out with the IDFILE when it is created.
    :param listopt: Option to substitute into LISTOPT to cause test listing
                    to take place.
    :param idoption: Option to substitute into cmd when supplying any test ids.
    :param repository: The repository to query for test times, if needed.
    :param parallel: If not True, prohibit parallel use : used to implement
                     --parallel run recursively.
    :param listpath: The file listing path to use. If None, a unique path
                     is created.
    :param test_filters: An optional list of test filters to apply. Each
        filter should be a string suitable for passing to re.compile.
        Filters are applied using search() rather than match(), so if
        anchoring is needed it should be included in the regex.
        The test ids used for executing are the union of all the
        individual filters: to take the intersection instead, craft a
        single regex that matches all your criteria. Filters are
        automatically applied by run_tests(), or can be applied by calling
        filter_tests(test_ids).
    :param group_callback: If supplied, should be a function that accepts a
        test id and returns a group id. A group id is an arbitrary value
        used as a dictionary key in the scheduler. All test ids with the
        same group id are scheduled onto the same backend test process.
    :param bool serial: Run tests serially
    :param path worker_path: Optional path of a manual worker grouping file
        to use for the run
    :param int concurrency: How many processes to use. The default (0)
        autodetects your CPU count and uses that.
    :param path exclude_list: Path to an exclusion list file, this file
        contains a separate regex exclude on each newline.
    :param path include_list: Path to an inclusion list file, this file
         contains a separate regex on each newline.
    :param boolean randomize: Randomize the test order after they are
        partitioned into separate workers
    :param bool dynamic: Use the experimental dynamic scheduler, workers
        will ask for the next test to run instead of being assigned a
        partition up front.
    :param bool dynamic_progress: Annotate the output of a dynamic run
        with the progress of the run (tests done, percentage and a rough
        ETA) next to each test. Off by default: the display is opt-in.
        Only used when dynamic is true.
    :param bool dynamic_debug: Enable the debug output of the dynamic
        scheduler (see the --dynamic-debug option of stestr run): each
        worker logs its progress to stderr and a watchdog thread
        reports the state of the run until it ends. Only used when
        dynamic is true.
    :param str test_path: The path of the test tree, used by the dynamic
        scheduler to run a full discovery in each worker, mirroring the
        non-dynamic runner subprocesses. Optional.
    :param str top_dir: The top level directory of the project, used
        together with test_path by the dynamic scheduler. Optional.
    """

    def __init__(
        self,
        test_ids,
        cmd_template,
        listopt,
        idoption,
        repository,
        parallel=True,
        listpath=None,
        test_filters=None,
        group_callback=None,
        serial=False,
        worker_path=None,
        concurrency=0,
        exclude_list=None,
        exclude_regex=None,
        include_list=None,
        randomize=False,
        dynamic=False,
        dynamic_progress=False,
        dynamic_debug=False,
        report_missing_ids=True,
        test_path=None,
        top_dir=None,
    ):
        """Create a TestProcessorFixture."""

        self.test_ids = test_ids
        self.report_missing_ids = report_missing_ids
        self.template = cmd_template
        self.listopt = listopt
        self.idoption = idoption
        self.repository = repository
        self.parallel = parallel
        if serial:
            self.parallel = False
        self._listpath = listpath
        self.test_filters = test_filters
        self._group_callback = group_callback
        self.worker_path = None
        self.worker_path = worker_path
        self.concurrency_value = concurrency
        self.exclude_list = exclude_list
        self.include_list = include_list
        self.exclude_regex = exclude_regex
        self.randomize = randomize
        self.dynamic = dynamic
        self.dynamic_progress = dynamic_progress
        self.dynamic_debug = dynamic_debug
        self._debug_stop = None
        self.test_path = test_path
        self.top_dir = top_dir

    def setUp(self):
        super().setUp()
        if self.dynamic:
            # One finalizer for the whole lifetime of the fixture: a
            # --until-failure run calls run_tests() repeatedly on the
            # same fixture, so the dynamic state of an iteration is
            # finished when the next iteration starts (see
            # _finish_dynamic_run), and this cleanup is the safety net
            # for the last iteration and for abnormal exits. Registering
            # the cleanups per run_tests() call would accumulate them
            # (and leak worker processes, stream fds and watchdog
            # threads) across the iterations.
            self.addCleanup(self._finish_dynamic_run)
            self._dynamic_join_timeout = _DYNAMIC_JOIN_TIMEOUT
        variable_regex = r"\$(IDOPTION|IDFILE|IDLIST|LISTOPT)"
        variables = {}
        list_variables = {"LISTOPT": self.listopt}
        cmd = self.template
        default_idstr = None

        def list_subst(match):
            return list_variables.get(match.groups(1)[0], "")

        self.list_cmd = re.sub(variable_regex, list_subst, cmd)
        nonparallel = not self.parallel
        selection_logic = (
            self.test_filters
            or self.exclude_list
            or self.include_list
            or self.exclude_regex
        )
        if nonparallel:
            self.concurrency = 1
        else:
            self.concurrency = None
            if self.concurrency_value:
                self.concurrency = int(self.concurrency_value)
            if not self.concurrency:
                self.concurrency = scheduler.local_concurrency()
            if not self.concurrency:
                self.concurrency = 1
        if self.dynamic:
            self._prepare_dynamic_hash_seed()
        if self.test_ids is None:
            if self.concurrency == 1:
                if default_idstr:
                    self.test_ids = default_idstr.split()
            if self.concurrency != 1 or selection_logic or self.worker_path:
                # Have to be able to tell each worker what to run / filter
                # tests.
                self.test_ids = self.list_tests()
        if self.test_ids is None:
            # No test ids to supply to the program.
            self.list_file_name = None
            name = ""
            idlist = ""
        else:
            self.test_ids = selection.construct_list(
                self.test_ids,
                exclude_list=self.exclude_list,
                include_list=self.include_list,
                regexes=self.test_filters,
                exclude_regex=self.exclude_regex,
            )
            name = self.make_listfile()
            variables["IDFILE"] = name
            idlist = " ".join(self.test_ids)
        variables["IDLIST"] = idlist

        def subst(match):
            return variables.get(match.groups(1)[0], "")

        if self.test_ids is None:
            # No test ids, no id option.
            idoption = ""
        else:
            idoption = re.sub(variable_regex, subst, self.idoption)
            variables["IDOPTION"] = idoption
        self.cmd = re.sub(variable_regex, subst, cmd)

    def _prepare_dynamic_hash_seed(self):
        """Share one hash seed between the children of a dynamic run.

        The listing process and the workers must generate the same test
        ids: ddt and similar test generators derive the generated ids
        from the iteration order of sets (e.g. @ddt.data(*a_frozenset)),
        which depends on the per-process hash seed. All the child
        processes inherit this environment, so they must share one seed.
        0 also keeps the ids stable across runs, which the repository
        data (timings, failures) relies on.

        "random" and the empty value make every child process pick its
        own seed, which makes the ids diverge between the workers, so
        they are treated as unset. Any other value the user set
        explicitly is honored. The variable is restored when the fixture
        is torn down, and a run that degenerates to the classic serial
        runner (concurrency 1) does not touch the environment at all,
        since it spawns no worker.
        """
        if self.concurrency == 1:
            return
        current = os.environ.get("PYTHONHASHSEED")
        if current is None or current in ("", "random"):
            self.useFixture(fixtures.EnvironmentVariable("PYTHONHASHSEED", "0"))

    def make_listfile(self):
        name = None
        try:
            if self._listpath:
                name = self._listpath
                stream = open(name, "wb")
            else:
                fd, name = tempfile.mkstemp()
                stream = os.fdopen(fd, "wb")
            with stream:
                self.list_file_name = name
                testlist.write_list(stream, self.test_ids)
        except Exception:
            if name:
                os.unlink(name)
            raise
        self.addCleanup(os.unlink, name)
        return name

    def _clear_SIGPIPE(self):
        """Clear SIGPIPE : child processes expect the default handler."""
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    def _start_process(self, cmd):
        # NOTE(claudiub): Windows does not support passing in a preexec_fn
        # argument.
        preexec_fn = None if sys.platform == "win32" else self._clear_SIGPIPE
        return subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            preexec_fn=preexec_fn,
        )

    def list_tests(self):
        """List the tests returned by list_cmd.

        :return: A list of test ids.
        """
        run_proc = self._start_process(self.list_cmd)
        out, err = run_proc.communicate()
        if run_proc.returncode != 0:
            sys.stdout.write(
                "\n=========================\n"
                "Failures during discovery"
                "\n=========================\n"
            )
            new_out = io.BytesIO()
            v2.ByteStreamToStreamResult(io.BytesIO(out), "stdout").run(
                results.CatFiles(new_out)
            )
            out = new_out.getvalue()
            if out:
                sys.stdout.write(out.decode("utf8"))
            if err:
                sys.stderr.write(err.decode("utf8"))
            sys.stdout.write(
                "\n" + "=" * 80 + "\n"
                "The above traceback was encountered during "
                "test discovery which imports all the found test"
                " modules in the specified test_path.\n"
            )
            exit(100)
        ids = testlist.parse_enumeration(out)
        return ids

    def _terminate_dynamic_workers(self, workers):
        """Collect the dynamic scheduler workers.

        Joins every worker, terminating (then killing) the ones that do
        not exit on their own: a run interrupted before the workers
        consumed every group (e.g. Ctrl-C, or all the workers crashed
        early) must not leave worker processes behind, and a worker
        blocked writing to a pipe nobody reads never exits on its own.
        """
        for worker in workers:
            proc = worker["proc"]
            proc.join(timeout=self._dynamic_join_timeout)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=self._dynamic_join_timeout)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=self._dynamic_join_timeout)

    def _finish_dynamic_run(self):
        """Finish a dynamic scheduler run.

        Stops the feed thread and the watchdog, collects the worker
        processes, closes their stream fds and releases the shared
        objects.

        Idempotent, and called from two places: at the beginning of
        every dynamic run_tests() invocation, to finish the previous
        iteration (a --until-failure run calls run_tests() repeatedly on
        the same fixture, and the streams of an iteration are only fully
        consumed after run_tests() returned, so the workers of an
        iteration can only be collected once the next iteration starts),
        and as a fixture cleanup, for the last iteration and for
        abnormal exits (Ctrl-C, crashed workers) where no further
        iteration happens.
        """
        feed_stop = getattr(self, "_dynamic_feed_stop", None)
        if feed_stop is not None:
            feed_stop.set()
            self._dynamic_feed_stop = None
        debug_stop = getattr(self, "_debug_stop", None)
        if debug_stop is not None:
            debug_stop.set()
            self._debug_stop = None
        workers = getattr(self, "_dynamic_workers", None) or []
        self._dynamic_workers = []
        self._terminate_dynamic_workers(workers)
        for worker in workers:
            stream = worker.get("stream")
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    # The stream may have been consumed and closed by the
                    # caller already.
                    pass
            worker["stream"] = None
        self._release_dynamic_shared_objects()

    def _release_dynamic_shared_objects(self):
        """Release the shared objects of the dynamic scheduler.

        Runs as part of _finish_dynamic_run, after the workers are gone.
        Dropping the references unlinks the underlying semaphores and
        unregisters them from the multiprocessing resource tracker right
        away, instead of at garbage collection or interpreter shutdown:
        the resource tracker exits as soon as every process of the run is
        gone, and an unregister message sent by a finalizer during the
        interpreter shutdown races with (and can be lost by) that exit,
        which makes the tracker warn about a leaked semaphore that was
        in fact already unlinked.
        """
        self._test_list_queue = None
        self._total_tests = None
        self._errors_reported = None

    def run_tests(self):
        """Run the tests defined by the command

        :return: A list of spawned processes.
        """
        result = []
        test_ids = self.test_ids
        # Handle the single worker case (this is also run recursively per
        # worker in the parallel case)
        if self.concurrency == 1 and (test_ids is None or test_ids):
            run_proc = self._start_process(self.cmd)
            # Prevent processes stalling if they read from stdin; we could
            # pass this through in future, but there is no point doing that
            # until we have a working can-run-debugger-inline story.
            run_proc.stdin.close()
            return [run_proc]
        if self.dynamic:
            # The dynamic scheduler groups the test ids itself: the
            # static partitions (and the worker file groups) would be
            # computed and then discarded.
            test_id_groups = None
        # If there is a worker path, use that to get worker groups
        elif self.worker_path:
            test_id_groups = scheduler.generate_worker_partitions(
                test_ids,
                self.worker_path,
                self.repository,
                self._group_callback,
                self.randomize,
            )
        # If we have multiple workers partition the tests and recursively
        # create single worker TestProcessorFixtures for each worker
        else:
            test_id_groups = scheduler.partition_tests(
                test_ids,
                self.concurrency,
                self.repository,
                self._group_callback,
                self.randomize,
            )
        if not self.dynamic:
            for test_ids in test_id_groups:
                if not test_ids:
                    # No tests in this partition
                    continue
                fixture = self.useFixture(
                    TestProcessorFixture(
                        test_ids,
                        self.template,
                        self.listopt,
                        self.idoption,
                        self.repository,
                        parallel=False,
                    )
                )
                result.extend(fixture.run_tests())
            return result
        else:
            # Finish the previous dynamic iteration, if any: a
            # --until-failure run calls run_tests() repeatedly on the
            # same fixture, and the streams of an iteration are only
            # fully consumed after run_tests() returned, so the workers
            # of an iteration can only be collected once the next
            # iteration starts (or at fixture cleanup, for the last
            # iteration).
            self._finish_dynamic_run()
            test_id_list = scheduler.get_dynamic_test_list(
                test_ids,
                self.repository,
                self._group_callback,
                self.randomize,
            )
            if not test_id_list:
                # Nothing to run, return an empty list of processes so the
                # caller can report that nothing matched.
                return result
            # Use spawn to launch a fresh interpreter and have the minimal
            # amount of state from stestr when invoking the test runner.
            # This is equivalent to non-dynamic mode using subprocess instead
            # of multiprocessing.
            context = multiprocessing.get_context("spawn")
            # NOTE(mtreinish): The queue is scoped to the instance so it
            # stays alive as long as the fixture does. If it was garbage
            # collected while the workers are still running, the finalizer
            # of its internal locks would unlink the underlying semaphores
            # and any worker that is still starting would crash with a
            # FileNotFoundError when rebuilding the queue.
            self._test_list_queue = context.Queue()
            # The parent never reads the queue back, and its feeder thread
            # must not hold the interpreter at exit when the workers
            # stopped consuming (e.g. an interrupted run, or workers dying
            # early): the pending groups are simply dropped.
            self._test_list_queue.cancel_join_thread()

            # The workers are started before the groups are fed to the
            # queue: the queue's internal semaphore is bounded by
            # SEM_VALUE_MAX, which is 32767 on macOS, so a run with more
            # groups than that would deadlock in put() with no consumer
            # to drain them. The workers pull the groups as they are fed.
            groups_done = None
            if self.dynamic_debug:
                groups_done = context.Value("i", 0)
            # The total number of tests of the run, exposed for the
            # parent to pass to the result rendering (the progress
            # annotation of the run output is computed from the subunit
            # stream itself, no shared counter is needed with the
            # workers).
            self._total_tests = sum(len(group) for group in test_id_list)
            # Shared flag for the discovery import errors: the first
            # worker that hits them reports them (the workers all
            # discover the same tree, so the errors would be repeated
            # once per worker otherwise).
            errors_reported = context.Value("b", 0)
            self._errors_reported = errors_reported
            # One worker per group at most: every worker runs a full
            # discovery of the test tree first, so spawning more workers
            # than groups would import the tree for nothing.
            num_workers = min(self.concurrency, len(test_id_list))
            # The workers are tracked on the instance, and the cleanup
            # collecting them was registered in setUp, before any of
            # them is started: a crash during the spawn loop must not
            # leave the already-started workers behind.
            self._dynamic_workers = []
            for i in range(num_workers):
                fd_pipe_r, fd_pipe_w = context.Pipe(False)
                proc = context.Process(
                    target=_dynamic_worker,
                    name="worker-%s" % i,
                    args=(
                        self._test_list_queue,
                        fd_pipe_w,
                        self.test_path,
                        self.top_dir,
                        self.dynamic_debug,
                        None,
                        groups_done,
                        self.report_missing_ids,
                        errors_reported,
                    ),
                )
                proc.start()
                # The parent never writes to the pipe. Close its write end
                # immediately so the reader sees EOF as soon as the workers
                # exit instead of only when the parent cleans up.
                fd_pipe_w.close()
                stream_read = os.fdopen(os.dup(fd_pipe_r.fileno()), "rb")
                fd_pipe_r.close()
                result.append({"stream": stream_read, "proc": proc})
                self._dynamic_workers.append(result[-1])
            # The groups are fed to the queue by a dedicated daemon
            # thread instead of the main thread. The workers may be
            # blocked writing to their stdout pipes, which the parent
            # only starts reading after run_tests() returned; once the
            # workers stop draining the queue, a synchronous feeding
            # loop would block the main thread here before it ever
            # reaches the read loop, deadlocking the whole run (the
            # queue's internal buffer is bounded, and its semaphore by
            # SEM_VALUE_MAX). The bounded put() timeout lets the feeder
            # outlive slow consumers, and the stop event lets the
            # finalizer abandon the feeding of an interrupted run.
            feed_stop = threading.Event()
            self._dynamic_feed_stop = feed_stop

            def _feed_groups():
                try:
                    for item in list(test_id_list) + [None] * num_workers:
                        while not feed_stop.is_set():
                            try:
                                self._test_list_queue.put(item, timeout=1)
                                break
                            except queue.Full:
                                continue
                        else:
                            return
                except Exception:
                    # The queue is broken (e.g. every worker died and
                    # its pipe was closed): the run is already failing
                    # and the parent will see the EOF on the worker
                    # streams, so drop the remaining groups.
                    pass

            self._dynamic_feed_thread = threading.Thread(
                target=_feed_groups,
                name="stestr-dynamic-feeder",
                daemon=True,
            )
            self._dynamic_feed_thread.start()
            if self.dynamic_debug:
                self._debug_stop = threading.Event()
                # The watchdog is stopped by the fixture finalizer, so
                # that it does not report one last time long after the
                # end of the run.
                threading.Thread(
                    target=_dynamic_debug_watchdog,
                    args=(result, groups_done, len(test_id_list)),
                    kwargs={"stop_event": self._debug_stop},
                    name="stestr-dynamic-debug-watchdog",
                    daemon=True,
                ).start()
            return result
