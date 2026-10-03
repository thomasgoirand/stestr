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
import re
import signal
import subprocess
import sys
import tempfile
import unittest

import fixtures
from subunit import v2

from stestr import results
from stestr import scheduler
from stestr import selection
from stestr.subunit_runner import program
from stestr.subunit_runner import run
from stestr import testlist


def _dynamic_worker(job_queue, subunit_pipe, test_path=None, top_dir=None):
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
    """
    # NOTE(mtreinish): Duplicate the file descriptor of the pipe so the
    # writer is decoupled from the Connection object's lifetime. Without
    # this you'll be fighting random bad file descriptor errors.
    subunit_stream = os.fdopen(os.dup(subunit_pipe.fileno()), "wb")
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
    # parent's console.
    sys.stdout = io.TextIOWrapper(subunit_stream)
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
            discovered = unittest.defaultTestLoader.discover(
                test_path, top_level_dir=top_dir
            )
            for test in program.iterate_tests(discovered):
                test_map[test.id()] = test
        while True:
            group = job_queue.get()
            if group is None:
                break
            if test_path:
                tests = [
                    test_map[test_id] for test_id in group if test_id in test_map
                ]
                if not tests:
                    continue
            else:
                tests = [
                    unittest.defaultTestLoader.loadTestsFromName(test_id)
                    for test_id in group
                ]
            # Run the group as a single suite, so that the class level
            # fixtures are handled by the suite like in the non-dynamic
            # mode.
            run.SubunitTestRunner(stdout=sys.stdout).run(unittest.TestSuite(tests))
    finally:
        sys.stdout.flush()
        subunit_stream.close()


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
        partition up front
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
        test_path=None,
        top_dir=None,
    ):
        """Create a TestProcessorFixture."""

        self.test_ids = test_ids
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
        self.test_path = test_path
        self.top_dir = top_dir

    def setUp(self):
        super().setUp()
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
                test_ids, self.concurrency, self.repository, self._group_callback
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
            test_id_list = scheduler.get_dynamic_test_list(
                test_ids, self.repository, self._group_callback
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

            for group in test_id_list:
                self._test_list_queue.put(group)
            # Enqueue a sentinel per worker. A worker exits when it receives
            # a sentinel, so every worker exits only after all the test ids
            # were handed to a worker.
            for _ in range(self.concurrency):
                self._test_list_queue.put(None)

            for i in range(self.concurrency):
                fd_pipe_r, fd_pipe_w = context.Pipe(False)
                proc = context.Process(
                    target=_dynamic_worker,
                    name="worker-%s" % i,
                    args=(
                        self._test_list_queue,
                        fd_pipe_w,
                        self.test_path,
                        self.top_dir,
                    ),
                )
                proc.start()
                # The parent never writes to the pipe. Close its write end
                # immediately so the reader sees EOF as soon as the workers
                # exit instead of only when the parent cleans up.
                fd_pipe_w.close()
                stream_read = os.dup(fd_pipe_r.fileno())
                fd_pipe_r.close()
                result.append({"stream": stream_read, "proc": proc})
            return result
