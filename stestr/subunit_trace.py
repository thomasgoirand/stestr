# Copyright 2014 Hewlett-Packard Development Company, L.P.
# Copyright 2014 Samsung Electronics
# All Rights Reserved.
#
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

"""Trace a subunit stream in reasonable detail and high accuracy."""
import argparse
import functools
import os
import re
import sys

import subunit
import testtools

from stestr import __version__
from stestr import colorizer
from stestr import results

# NOTE(mtreinish) on python3 anydbm was renamed dbm and the python2 dbm module
# was renamed to dbm.ndbm, this block takes that into account
try:
    import anydbm as dbm
except ImportError:
    import dbm

DAY_SECONDS = 60 * 60 * 24
FAILS = []
RESULTS = {}

# The test statuses that mark the end of a test, used by the progress
# annotation of a dynamic scheduler run (see DynamicProgress).
_PROGRESS_FINAL_STATUSES = frozenset(
    ["success", "fail", "skip", "xfail", "uxsuccess", "uxfail"]
)


def _format_progress_duration(seconds):
    """Format a duration as ~HH:MM:SS for a progress display."""
    seconds = int(seconds)
    return "~%02d:%02d:%02d" % (seconds // 3600, (seconds % 3600) // 60, seconds % 60)


def _format_elapsed_seconds(seconds):
    """Format a measured duration as HH:MM:SS, without the ETA tilde."""
    seconds = int(seconds)
    return "%02d:%02d:%02d" % (seconds // 3600, (seconds % 3600) // 60, seconds % 60)


class DynamicProgress:
    """Compute the progress annotation of a dynamic scheduler run.

    With --dynamic-progress, each test line of the run output is
    annotated with the progress of the run: the number of tests already
    completed (the test on the line included), the corresponding
    percentage and a rough ETA extrapolating the average duration of the
    completed tests to the remaining ones, e.g.
    ``[ 42/312 (13.5%) Time: 00:00:33, ETA: ~00:08:53]``.

    The numbers are computed from the completion timestamps the subunit
    stream carries, so the annotation needs no shared state with the
    workers of the run. The elapsed time and the ETA are unknown until a
    first test completed.

    :param int total_tests: The total number of tests scheduled for the
        run.
    """

    def __init__(self, total_tests):
        self.total_tests = total_tests
        self.done = 0
        self.start_time = None
        self.last_time = None

    def record_start(self, timestamp=None):
        """Account the start timestamp of a test of the run."""
        if timestamp is not None and (
            self.start_time is None or timestamp < self.start_time
        ):
            self.start_time = timestamp

    def record_stop(self, timestamp=None):
        """Account a completed test and its completion timestamp."""
        if timestamp is not None:
            self.last_time = timestamp
        self.done += 1

    def elapsed_time(self):
        """Render the time elapsed since the first test started."""
        if self.start_time is None or self.last_time is None:
            return "unknown"
        return _format_elapsed_seconds(
            (self.last_time - self.start_time).total_seconds()
        )

    def eta(self):
        elapsed = None
        if self.start_time is not None and self.last_time is not None:
            elapsed = (self.last_time - self.start_time).total_seconds()
        done = min(max(self.done, 0), self.total_tests)
        if not self.total_tests or not done:
            return "unknown"
        if done >= self.total_tests:
            return "~00:00:00"
        return _format_progress_duration(elapsed / done * (self.total_tests - done))

    def __str__(self):
        done = min(max(self.done, 0), self.total_tests)
        percent = 100.0 * done / self.total_tests if self.total_tests else 100.0
        return "[ %d/%d (%.1f%%) Time: %s, ETA: %s]" % (
            done,
            self.total_tests,
            percent,
            self.elapsed_time(),
            self.eta(),
        )


class DynamicProgressReporter(testtools.StreamResult):
    """Emit the progress annotation of a run on stderr, per test.

    Used on the raw subunit stream path (--subunit): the per-test lines
    are rendered by the consumer of the stream, which stestr cannot
    annotate, so the same annotation as the subunit-trace one is written
    to stderr for every completed test, interleaved with the output of
    the pipeline.

    :param progress: The DynamicProgress instance accounting the run.
    :param stream: The stream the annotations are written to, defaults
        to sys.stderr.
    """

    def __init__(self, progress, stream=None):
        super().__init__()
        self._progress = progress
        self._stream = stream if stream is not None else sys.stderr

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
            self._progress.record_start(timestamp)
        elif test_status in _PROGRESS_FINAL_STATUSES:
            self._progress.record_stop(timestamp)
            self._stream.write("%s %s\n" % (self._progress, test_id))
            self._stream.flush()


def total_seconds(timedelta):
    # NOTE(mtreinish): This method is built-in to the timedelta class in
    # python >= 2.7 it is here to enable it's use on older versions
    return (
        (timedelta.days * DAY_SECONDS + timedelta.seconds) * 10**6
        + timedelta.microseconds
    ) / 10**6


def cleanup_test_name(name, strip_tags=True, strip_scenarios=False):
    """Clean up the test name for display.

    By default we strip out the tags in the test because they don't help us
    in identifying the test that is run to it's result.

    Make it possible to strip out the testscenarios information (not to
    be confused with tempest scenarios) however that's often needed to
    indentify generated negative tests.
    """
    if strip_tags:
        tags_start = name.find("[")
        tags_end = name.find("]")
        if tags_start > 0 and tags_end > tags_start:
            newname = name[:tags_start]
            newname += name[tags_end + 1 :]
            name = newname

    if strip_scenarios:
        tags_start = name.find("(")
        tags_end = name.find(")")
        if tags_start > 0 and tags_end > tags_start:
            newname = name[:tags_start]
            newname += name[tags_end + 1 :]
            name = newname

    return name


def get_duration(timestamps):
    start, end = timestamps
    if not start or not end:
        duration = ""
    else:
        delta = end - start
        duration = "%d.%06ds" % (
            delta.days * DAY_SECONDS + delta.seconds,
            delta.microseconds,
        )
    return duration


def find_worker(test):
    """Get the worker number.

    If there are no workers because we aren't in a concurrent environment,
    assume the worker number is 0.
    """
    for tag in test["tags"]:
        if tag.startswith("worker-"):
            return int(tag[7:])
    return 0


def worker_tag(worker, worker_count=1):
    """Render the worker prefix of a result line.

    The number is right-aligned on the width of the largest worker
    number of the run, so that the result lines line up: a run with
    less than 10 workers uses one character, more than 99 three.
    """
    width = max(len(str(max(worker_count - 1, 0))), 1)
    return "{{{}}}".format(str(worker).rjust(width))


class DiscardAttachments(testtools.StreamResult):
    """Swallow the file attachment events of a subunit stream.

    The runner streams carry the stdout/stderr of the tests as file
    attachments, in chunks without a test id. Their relevant content is
    already rendered by the per-test output of the failing tests, and
    tests often print unstructured data (e.g. generated resource names)
    without trailing newlines, which catted verbatim would corrupt the
    rendering of the next result line. Dropping the raw chunks keeps
    the output aligned, like the subunit2pyunit pipelines do.
    """

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
        pass


# Print out stdout/stderr if it exists, always
def print_attachments(stream, test, all_channels=False, show_binary_attachments=False):
    """Print out subunit attachments.

    Print out subunit attachments that contain content. This
    runs in 2 modes, one for successes where we print out just stdout
    and stderr, and an override that dumps all the attachments.
    """
    channels = ("stdout", "stderr")
    for name, detail in test["details"].items():
        # NOTE(sdague): the subunit names are a little crazy, and actually
        # are in the form pythonlogging:'' (with the colon and quotes)
        name = name.split(":")[0]
        if detail.content_type.type == "test":
            detail.content_type.type = "text"
        if all_channels or name in channels:
            title = "Captured %s:" % name
            stream.write("\n{}\n{}\n".format(title, ("~" * len(title))))

            # indent attachment lines 4 spaces to make them visually
            # offset
            if detail.content_type.type == "text":
                for line in detail.iter_text():
                    stream.write("    %s\n" % line)
            elif show_binary_attachments:  # binary
                for line in detail.iter_bytes():
                    stream.write("    %s\n" % line)


def find_test_run_time_diff(test_id, run_time):
    times_db_path = os.path.join(
        os.path.join(os.getcwd(), ".testrepository"), "times.dbm"
    )
    if os.path.isfile(times_db_path):
        try:
            test_times = dbm.open(times_db_path)
        except Exception:
            return False
        try:
            avg_runtime = float(test_times.get(str(test_id), False))
        except Exception:
            try:
                avg_runtime = float(test_times[str(test_id)])
            except Exception:
                avg_runtime = False

        if avg_runtime and avg_runtime > 0:
            run_time = float(run_time.rstrip("s"))
            perc_diff = ((run_time - avg_runtime) / avg_runtime) * 100
            return perc_diff
    return False


def show_outcome(
    stream,
    test,
    print_failures=False,
    failonly=False,
    enable_diff=False,
    threshold="0",
    abbreviate=False,
    enable_color=False,
    suppress_attachments=False,
    all_attachments=False,
    show_binary_attachments=True,
    progress=None,
    worker_count=1,
):
    global RESULTS
    status = test["status"]
    # TODO(sdague): ask lifeless why on this?
    if status == "exists":
        return

    worker = find_worker(test)
    name = cleanup_test_name(test["id"])
    duration = get_duration(test["timestamps"])

    if worker not in RESULTS:
        RESULTS[worker] = []
    RESULTS[worker].append(test)

    # don't count the end of the return code as a fail
    if name == "process-returncode":
        return

    # The progress annotation of a dynamic scheduler run, displayed at
    # the beginning of every test line: the test on the line is counted
    # as completed, and the start timestamps of the tests bound the
    # elapsed time of the run.
    if progress is not None:
        timestamps = test.get("timestamps") or []
        if timestamps:
            progress.record_start(timestamps[0])
            progress.record_stop(timestamps[-1])
        else:
            progress.record_stop(None)
    annotation = "%s " % progress if progress is not None else ""

    for color in [colorizer.AnsiColorizer, colorizer.NullColorizer]:
        if not enable_color:
            color = colorizer.NullColorizer(stream)
            break
        if color.supported():
            color = color(stream)
            break

    if status == "fail" or status == "uxsuccess":
        FAILS.append(test)
        if abbreviate:
            color.write("F", "red")
        else:
            stream.write(
                "{} {}{} [{}] ... ".format(
                    worker_tag(worker, worker_count), annotation, name, duration
                )
            )
            color.write("FAILED", "red")
            stream.write("\n")
            if not print_failures:
                print_attachments(
                    stream,
                    test,
                    all_channels=True,
                    show_binary_attachments=show_binary_attachments,
                )
    elif not failonly:
        if status == "success" or status == "xfail":
            if abbreviate:
                color.write(".", "green")
            else:
                out_string = "{} {}{} [{}".format(
                    worker_tag(worker, worker_count), annotation, name, duration
                )
                perc_diff = find_test_run_time_diff(test["id"], duration)
                if enable_diff:
                    if perc_diff and abs(perc_diff) >= abs(float(threshold)):
                        if perc_diff > 0:
                            out_string = out_string + " +%.2f%%" % perc_diff
                        else:
                            out_string = out_string + " %.2f%%" % perc_diff
                stream.write(out_string + "] ... ")
                color.write("ok", "green")
                stream.write("\n")
                if not suppress_attachments:
                    print_attachments(
                        stream,
                        test,
                        all_channels=all_attachments,
                        show_binary_attachments=show_binary_attachments,
                    )
        elif status == "skip":
            if abbreviate:
                color.write("S", "blue")
            else:
                reason = test["details"].get("reason", "")
                if reason:
                    reason = ": " + reason.as_text()
                stream.write(
                    "{} {}{} ... ".format(
                        worker_tag(worker, worker_count), annotation, name
                    )
                )
                color.write("SKIPPED", "blue")
                stream.write("%s" % (reason))
                stream.write("\n")
        else:
            if abbreviate:
                stream.write("%s" % test["status"][0])
            else:
                stream.write(
                    "{} {}{} [{}] ... {}\n".format(
                        worker_tag(worker, worker_count),
                        annotation,
                        name,
                        duration,
                        test["status"],
                    )
                )
                if not print_failures:
                    print_attachments(
                        stream,
                        test,
                        all_channels=True,
                        show_binary_attachments=show_binary_attachments,
                    )

    stream.flush()


def print_fails(stream):
    """Print summary failure report."""
    if not FAILS:
        return
    stream.write("\n==============================\n")
    stream.write("Failed %s tests - output below:" % len(FAILS))
    stream.write("\n==============================\n")
    for f in FAILS:
        stream.write("\n%s\n" % f["id"])
        stream.write("%s\n" % ("-" * len(f["id"])))
        print_attachments(stream, f, all_channels=True)
    stream.write("\n")


def count_tests(key, value):
    count = 0
    for k, v in RESULTS.items():
        for item in v:
            if key in item:
                if re.search(value, item[key]):
                    count += 1
    return count


def get_stuck_in_progress():
    key = "status"
    match = re.compile("^inprogress$")
    in_progress = []
    for k, v in RESULTS.items():
        for item in v:
            if key in item:
                if match.search(item[key]):
                    in_progress.append(item["id"])
    return in_progress


def run_time():
    runtime = 0.0
    for k, v in RESULTS.items():
        for test in v:
            test_dur = get_duration(test["timestamps"]).strip("s")
            # NOTE(toabctl): get_duration() can return an empty string
            # which leads to a ValueError when casting to float
            if test_dur:
                runtime += float(test_dur)
    return runtime


def worker_stats(worker):
    tests = RESULTS[worker]
    num_tests = len(tests)
    stop_time = tests[-1]["timestamps"][1]
    start_time = tests[0]["timestamps"][0]
    if not start_time or not stop_time:
        delta = "N/A"
    else:
        delta = stop_time - start_time
    return num_tests, str(delta)


def print_summary(stream, elapsed_time):
    stream.write("\n======\nTotals\n======\n")
    stream.write(
        "Ran: {} tests in {:.4f} sec.\n".format(
            count_tests("status", ".*"), total_seconds(elapsed_time)
        )
    )
    stream.write(" - Passed: %s\n" % count_tests("status", "^success$"))
    stream.write(" - Skipped: %s\n" % count_tests("status", "^skip$"))
    stream.write(" - Expected Fail: %s\n" % count_tests("status", "^xfail$"))
    stream.write(" - Unexpected Success: %s\n" % count_tests("status", "^uxsuccess$"))
    stream.write(" - Failed: %s\n" % count_tests("status", "^fail$"))
    stream.write("Sum of execute time for each test: %.4f sec.\n" % run_time())

    # we could have no results, especially as we filter out the process-codes
    if RESULTS:
        stream.write("\n==============\nWorker Balance\n==============\n")

        for w in range(max(RESULTS.keys()) + 1):
            if w not in RESULTS:
                stream.write(" - WARNING: missing Worker %s!\n" % w)
            else:
                num, time = worker_stats(w)
                out_str = " - Worker {} ({} tests) => {}".format(w, num, time)
                if time.isdigit():
                    out_str += "s"
                out_str += "\n"
                stream.write(out_str)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version="%s" % __version__)
    parser.add_argument(
        "--no-failure-debug",
        "-n",
        action="store_true",
        dest="print_failures",
        help="Disable printing failure " "debug information in realtime",
    )
    parser.add_argument(
        "--fails",
        "-f",
        action="store_true",
        dest="post_fails",
        help="Print failure debug " "information after the stream is proccesed",
    )
    parser.add_argument(
        "--failonly",
        action="store_true",
        dest="failonly",
        help="Don't print success items",
        default=(os.environ.get("TRACE_FAILONLY", False) is not False),
    )
    parser.add_argument(
        "--abbreviate",
        "-a",
        action="store_true",
        dest="abbreviate",
        help="Print one character status" "for each test",
    )
    parser.add_argument(
        "--perc-diff",
        "-d",
        action="store_true",
        dest="enable_diff",
        help="Print percent change in run time on each test ",
    )
    parser.add_argument(
        "--diff-threshold",
        "-t",
        dest="threshold",
        help="Threshold to use for displaying percent change "
        "from the avg run time. If one is not specified "
        "the percent change will always be displayed",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Don't print the summary of the test run after " " completes",
    )
    parser.add_argument(
        "--color", action="store_true", help="Print results with colors"
    )
    return parser.parse_args()


def trace(
    stdin,
    stdout,
    print_failures=False,
    failonly=False,
    enable_diff=False,
    abbreviate=False,
    color=False,
    post_fails=False,
    no_summary=False,
    suppress_attachments=False,
    all_attachments=False,
    show_binary_attachments=False,
    progress=None,
    worker_count=1,
):
    stream = subunit.ByteStreamToStreamResult(stdin, non_subunit_name="stdout")
    # Reset the module level state so that a trace run is not polluted by
    # any previous run in the same process (e.g. when the dynamic scheduler
    # runs several tests in the same worker process).
    RESULTS.clear()
    FAILS.clear()
    outcomes = testtools.StreamToDict(
        functools.partial(
            show_outcome,
            stdout,
            print_failures=print_failures,
            failonly=failonly,
            enable_diff=enable_diff,
            abbreviate=abbreviate,
            enable_color=color,
            suppress_attachments=suppress_attachments,
            all_attachments=all_attachments,
            show_binary_attachments=show_binary_attachments,
            progress=progress,
            worker_count=worker_count,
        )
    )
    summary = testtools.StreamSummary()
    result = testtools.CopyStreamResult([outcomes, summary])
    result = testtools.StreamResultRouter(result)
    cat = DiscardAttachments()
    result.add_rule(cat, "test_id", test_id=None)
    result.startTestRun()
    try:
        stream.run(result)
    finally:
        result.stopTestRun()
    start_times = []
    stop_times = []
    for worker in RESULTS:
        start_times += [
            x["timestamps"][0]
            for x in RESULTS[worker]
            if x["timestamps"][0] is not None
        ]
        stop_times += [
            x["timestamps"][1]
            for x in RESULTS[worker]
            if x["timestamps"][1] is not None
        ]
    if print_full_output(stdout, start_times, stop_times, post_fails, no_summary):
        return 1
    return 0 if results.wasSuccessful(summary) else 1


def print_full_output(stdout, start_times, stop_times, post_fails, no_summary):
    """Print output plus edge case validation"""
    if not start_times or count_tests("status", ".*") == 0:
        print("The test run didn't actually run any tests", file=sys.stderr)
        return 1

    start_time = min(start_times)
    stop_time = max(stop_times)
    elapsed_time = stop_time - start_time

    if post_fails:
        print_fails(stdout)
    if not no_summary:
        print_summary(stdout, elapsed_time)

    # NOTE(mtreinish): Ideally this should live in testtools streamSummary
    # this is just in place until the behavior lands there (if it ever does)
    if count_tests("status", "^xfail$") + count_tests("status", "^success$") == 0:
        print("\nNo tests were successful during the run", file=sys.stderr)
        return 1
    in_progress = get_stuck_in_progress()
    if in_progress:
        print(
            "\nThe following tests exited without returning a status\n"
            "and likely segfaulted or crashed Python:",
            file=sys.stderr,
        )
        for test in in_progress:
            print("\n\t* %s" % test, file=sys.stderr)
        return 1


def main():
    args = parse_args()
    exit(
        trace(
            sys.stdin,
            sys.stdout,
            args.print_failures,
            args.failonly,
            args.enable_diff,
            args.abbreviate,
            args.color,
            args.post_fails,
            args.no_summary,
        )
    )


if __name__ == "__main__":
    main()
