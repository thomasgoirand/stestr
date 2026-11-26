# Copyright 2015 SUSE Linux GmbH
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

from datetime import datetime as dt
from datetime import timedelta
import io
import os
import sys
from unittest.mock import patch

from ddt import data
from ddt import ddt
from ddt import unpack
from iso8601 import UTC
from subunit.v2 import StreamResultToBytes

from stestr import subunit_trace
from stestr.tests import base


@ddt
class TestSubunitTrace(base.TestCase):
    def setUp(self):
        super().setUp()
        # NOTE(mtreinish): subunit-trace relies on a global to track results
        # with the expectation that it's run once per python interpreter
        # (like per stestr run or other command). Make sure to clear those on
        # each test to isolate the tests from each other.
        subunit_trace.RESULTS = {}
        subunit_trace.FAILS = []

    @data(
        (
            [dt(2015, 4, 17, 22, 23, 14, 111111), dt(2015, 4, 17, 22, 23, 14, 111111)],
            "0.000000s",
        ),
        (
            [dt(2015, 4, 17, 22, 23, 14, 111111), dt(2015, 4, 17, 22, 23, 15, 111111)],
            "1.000000s",
        ),
        ([dt(2015, 4, 17, 22, 23, 14, 111111), None], ""),
    )
    @unpack
    def test_get_durating(self, timestamps, expected_result):
        self.assertEqual(subunit_trace.get_duration(timestamps), expected_result)

    @data(
        (
            [dt(2015, 4, 17, 22, 23, 14, 111111), dt(2015, 4, 17, 22, 23, 14, 111111)],
            0.0,
        ),
        (
            [dt(2015, 4, 17, 22, 23, 14, 111111), dt(2015, 4, 17, 22, 23, 15, 111111)],
            1.0,
        ),
        ([dt(2015, 4, 17, 22, 23, 14, 111111), None], 0.0),
    )
    @unpack
    def test_run_time(self, timestamps, expected_result):
        patched_res = {0: [{"timestamps": timestamps}]}
        with patch.dict(subunit_trace.RESULTS, patched_res, clear=True):
            self.assertEqual(subunit_trace.run_time(), expected_result)

    def test_trace(self):
        regular_stream = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sample_streams/successful.subunit",
        )
        bytes_ = io.BytesIO()
        with open(regular_stream, "rb") as stream:
            bytes_.write(bytes(stream.read()))
        bytes_.seek(0)
        stdin = io.TextIOWrapper(io.BufferedReader(bytes_))
        returncode = subunit_trace.trace(stdin, sys.stdout)
        self.assertEqual(0, returncode)

    def test_trace_with_all_skips(self):
        regular_stream = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "sample_streams/all_skips.subunit",
        )
        bytes_ = io.BytesIO()
        with open(regular_stream, "rb") as stream:
            bytes_.write(bytes(stream.read()))
        bytes_.seek(0)
        stdin = io.TextIOWrapper(io.BufferedReader(bytes_))
        returncode = subunit_trace.trace(stdin, sys.stdout)
        self.assertEqual(1, returncode)

    def test_trace_with_failures(self):
        regular_stream = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "sample_streams/failure.subunit"
        )
        bytes_ = io.BytesIO()
        with open(regular_stream, "rb") as stream:
            bytes_.write(bytes(stream.read()))
        bytes_.seek(0)
        stdin = io.TextIOWrapper(io.BufferedReader(bytes_))
        returncode = subunit_trace.trace(stdin, sys.stdout)
        self.assertEqual(1, returncode)

    def test_trace_with_stuck_inprogress(self):
        output = io.BytesIO()
        stream = StreamResultToBytes(output)
        stream.startTestRun()
        stream.status(
            test_id="test_passes", test_status="inprogress", timestamp=dt.now(UTC)
        )
        stream.status(
            test_id="test_segfault", test_status="inprogress", timestamp=dt.now(UTC)
        )
        stream.status(
            test_id="test_passes", test_status="success", timestamp=dt.now(UTC)
        )
        stream.stopTestRun()
        output.seek(0)
        # capture stderr for test
        stderr = io.StringIO()
        sys_err = sys.stderr
        sys.stderr = stderr

        def restore_stderr():
            sys.stderr = sys_err

        self.addCleanup(restore_stderr)
        stdin = io.TextIOWrapper(io.BufferedReader(output))
        returncode = subunit_trace.trace(stdin, sys.stdout)
        self.assertEqual(1, returncode)
        stderr.seek(0)
        expected = """
The following tests exited without returning a status
and likely segfaulted or crashed Python:

\t* test_segfault
"""
        self.assertEqual(stderr.read(), expected)


class TestDynamicProgress(base.TestCase):
    def test_unknown_before_first_completion(self):
        # The ETA is unknown until a first test completed.
        progress = subunit_trace.DynamicProgress(312)
        self.assertEqual(
            "[ 0/312 (0.0%) Time: unknown, ETA: unknown]", str(progress)
        )

    def test_mid_run(self):
        # 42 done tests over 312, 83s elapsed since the first test
        # started: the ETA extrapolates the average duration of the done
        # tests to the 270 remaining ones.
        progress = subunit_trace.DynamicProgress(312)
        start = dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC)
        progress.record_start(start)
        for i in range(42):
            progress.record_stop(start + timedelta(seconds=83.0 * (i + 1) / 42))
        self.assertEqual(
            "[ 42/312 (13.5%) Time: 00:01:23, ETA: ~00:08:53]", str(progress)
        )

    def test_completion(self):
        progress = subunit_trace.DynamicProgress(312)
        start = dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC)
        progress.record_start(start)
        for i in range(312):
            progress.record_stop(start + timedelta(seconds=600.0 * (i + 1) / 312))
        self.assertEqual(
            "[ 312/312 (100.0%) Time: 00:10:00, ETA: ~00:00:00]", str(progress)
        )

    def test_rounds_the_percentage(self):
        progress = subunit_trace.DynamicProgress(3)
        start = dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC)
        progress.record_start(start)
        progress.record_stop(start)
        progress.record_stop(start + timedelta(seconds=9))
        self.assertEqual(
            "[ 2/3 (66.7%) Time: 00:00:09, ETA: ~00:00:04]", str(progress)
        )

    def test_clamps_out_of_range_counts(self):
        progress = subunit_trace.DynamicProgress(3)
        start = dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC)
        progress.record_start(start)
        for i in range(5):
            progress.record_stop(start + timedelta(seconds=i))
        self.assertEqual(
            "[ 3/3 (100.0%) Time: 00:00:04, ETA: ~00:00:00]", str(progress)
        )


class TestDynamicProgressInTrace(base.TestCase):
    def setUp(self):
        super().setUp()
        subunit_trace.RESULTS = {}
        subunit_trace.FAILS = []

    @staticmethod
    def _two_test_stream():
        output = io.BytesIO()
        stream = StreamResultToBytes(output)
        stream.startTestRun()
        stream.status(
            test_id="testA",
            test_status="inprogress",
            timestamp=dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC),
        )
        stream.status(
            test_id="testA",
            test_status="success",
            timestamp=dt(2026, 11, 26, 10, 0, 1, tzinfo=UTC),
        )
        stream.status(
            test_id="testB",
            test_status="inprogress",
            timestamp=dt(2026, 11, 26, 10, 0, 2, tzinfo=UTC),
        )
        stream.status(
            test_id="testB",
            test_status="fail",
            timestamp=dt(2026, 11, 26, 10, 0, 4, tzinfo=UTC),
        )
        stream.stopTestRun()
        output.seek(0)
        return output

    def test_trace_annotates_every_test_line(self):
        stdin = io.TextIOWrapper(io.BufferedReader(self._two_test_stream()))
        raw = io.BytesIO()
        out = io.TextIOWrapper(raw)
        returncode = subunit_trace.trace(
            stdin, out, progress=subunit_trace.DynamicProgress(2)
        )
        out.flush()
        self.assertEqual(1, returncode)
        # The annotation stands at the beginning of the line, between
        # the worker and the test id, on passing and failing tests
        # alike.
        self.assertIn(
            "[ 1/2 (50.0%) Time: 00:00:01, ETA: ~00:00:01] testA",
            raw.getvalue().decode(),
        )
        self.assertIn(
            "[ 2/2 (100.0%) Time: 00:00:04, ETA: ~00:00:00] testB",
            raw.getvalue().decode(),
        )

    def test_trace_without_progress_is_not_annotated(self):
        stdin = io.TextIOWrapper(io.BufferedReader(self._two_test_stream()))
        raw = io.BytesIO()
        out = io.TextIOWrapper(raw)
        returncode = subunit_trace.trace(stdin, out)
        out.flush()
        self.assertEqual(1, returncode)
        self.assertNotIn("ETA:", raw.getvalue().decode())

    def test_progress_reporter(self):
        # On the raw subunit stream path the same annotation is written
        # to stderr for every completed test.
        progress = subunit_trace.DynamicProgress(2)
        out = io.StringIO()
        reporter = subunit_trace.DynamicProgressReporter(progress, stream=out)
        reporter.status(
            test_id="testA",
            test_status="inprogress",
            timestamp=dt(2026, 11, 26, 10, 0, 0, tzinfo=UTC),
        )
        reporter.status(
            test_id="testA",
            test_status="success",
            timestamp=dt(2026, 11, 26, 10, 0, 1, tzinfo=UTC),
        )
        reporter.status(
            test_id="testB",
            test_status="inprogress",
            timestamp=dt(2026, 11, 26, 10, 0, 2, tzinfo=UTC),
        )
        reporter.status(
            test_id="testB",
            test_status="fail",
            timestamp=dt(2026, 11, 26, 10, 0, 4, tzinfo=UTC),
        )
        self.assertEqual(
            [
                "[ 1/2 (50.0%) Time: 00:00:01, ETA: ~00:00:01] testA",
                "[ 2/2 (100.0%) Time: 00:00:04, ETA: ~00:00:00] testB",
            ],
            out.getvalue().splitlines(),
        )

    def test_trace_aligns_on_the_worker_count(self):
        stdin = io.TextIOWrapper(io.BufferedReader(self._two_test_stream()))
        raw = io.BytesIO()
        out = io.TextIOWrapper(raw)
        returncode = subunit_trace.trace(
            stdin, out, progress=subunit_trace.DynamicProgress(2), worker_count=12
        )
        out.flush()
        self.assertEqual(1, returncode)
        self.assertIn(
            "{ 0} [ 1/2 (50.0%) Time: 00:00:01, ETA: ~00:00:01] testA",
            raw.getvalue().decode(),
        )


class TestWorkerTag(base.TestCase):
    def test_unknown_worker_count_is_unpadded(self):
        self.assertEqual("{2}", subunit_trace.worker_tag(2))

    def test_single_digit_workers_are_unpadded(self):
        self.assertEqual("{0}", subunit_trace.worker_tag(0, 1))
        self.assertEqual("{2}", subunit_trace.worker_tag(2, 10))

    def test_two_digit_workers_are_right_aligned(self):
        self.assertEqual("{ 2}", subunit_trace.worker_tag(2, 12))
        self.assertEqual("{19}", subunit_trace.worker_tag(19, 12))

    def test_hundred_workers_use_three_characters(self):
        self.assertEqual("{99}", subunit_trace.worker_tag(99, 100))
        self.assertEqual("{  2}", subunit_trace.worker_tag(2, 101))
        self.assertEqual("{100}", subunit_trace.worker_tag(100, 101))


class TestDiscardAttachments(base.TestCase):
    def test_attachment_chunks_are_dropped(self):
        discard = subunit_trace.DiscardAttachments()
        discard.status(file_name="stdout", file_bytes=b"._cleleue")
        discard.status(file_name="stdout", file_bytes=b"more")
        discard.status(file_name="stdout", file_bytes=b"")

    def test_test_events_are_ignored(self):
        discard = subunit_trace.DiscardAttachments()
        discard.status(
            test_id="testA",
            test_status="success",
            file_name=None,
            file_bytes=None,
        )
