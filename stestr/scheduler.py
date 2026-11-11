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

import collections
import itertools
import multiprocessing
import operator
import random

import yaml

from stestr import selection


def _load_time_data(test_ids, repository):
    """Load the recorded durations of the test ids from the repository.

    :param list test_ids: The list of test_ids to look up
    :param repository: A repository object used for looking up the timing
        data of the tests. Optional: without it every test id is treated
        as unknown.
    :return: A (timed_tests, unknown_tests) tuple: the dict of the known
        test durations and the set of the ids without any recorded
        duration.
    """
    if repository:
        time_data = repository.get_test_times(test_ids)
        return time_data["known"], time_data["unknown"]
    return {}, set(test_ids)


def _group_test_ids(test_ids, group_callback):
    """Group the test ids by group id.

    :param list test_ids: The list of test_ids to be grouped
    :param group_callback: A callback function that is used as a scheduler
        hint to group test_ids together and treat them as a single unit for
        scheduling. This function expects a single test_id parameter and it
        will return a group identifier. Tests_ids that have the same group
        identifier will be kept together. Optional: without it every test
        id is a group of its own.
    :return: A collections.defaultdict mapping each group id to the list of
        the test ids of the group, in the order they appear in test_ids.
    """
    if group_callback is None:

        def group_callback(_):
            return None

    # Group tests: generate group_id -> test_ids.
    group_ids = collections.defaultdict(list)
    for test_id in test_ids:
        group_id = group_callback(test_id) or test_id
        group_ids[group_id].append(test_id)
    return group_ids


def _time_groups(group_ids, timed_tests, unknown_tests):
    """Time the groups of test ids.

    :param dict group_ids: A group_id -> test_ids dict, as returned by
        _group_test_ids().
    :param dict timed_tests: The dict of the known test durations.
    :param set unknown_tests: The set of the ids without any recorded
        duration.
    :return: A (timed, partial, unknown) tuple: a dict(group_id -> time)
        of the fully timed groups, a dict(group_id -> time) of the
        partially timed groups, and the list of the group ids of the
        groups without any timed test. We may in future treat partially
        timed groups differently for scheduling, but at least today we
        just schedule them after the fully timed groups.
    """
    timed = {}
    partial = {}
    unknown = []
    for group_id, group_tests in group_ids.items():
        untimed_ids = unknown_tests.intersection(group_tests)
        group_time = sum(
            [
                timed_tests[test_id]
                for test_id in untimed_ids.symmetric_difference(group_tests)
            ]
        )
        if not untimed_ids:
            timed[group_id] = group_time
        elif group_time:
            partial[group_id] = group_time
        else:
            unknown.append(group_id)
    return timed, partial, unknown


def get_dynamic_test_list(
    test_ids, repository=None, group_callback=None, randomize=False
):
    """Group the test ids for the dynamic scheduler.

    The ids are returned as a list of groups of test ids, sorted from the
    slowest to the fastest estimated group. Each group is the unit of work
    a dynamic scheduler worker picks from the queue: with a group_callback
    (or a group_regex config) a group is what the non-dynamic scheduler
    keeps on a single worker, e.g. a test class, so that the class level
    fixtures are only run once per worker like in the non-dynamic mode.

    :param list test_ids: The list of test_ids to be scheduled
    :param repository: A repository object used for looking up the timing
        data of the tests. Optional.
    :param group_callback: A callback function that is used as a scheduler
        hint to group test_ids together and treat them as a single unit for
        scheduling. This function expects a single test_id parameter and it
        will return a group identifier. Tests_ids that have the same group
        identifier will be kept together on a worker. Optional.
    :param bool randomize: If true the order in which the groups are
        picked from the queue is randomized, as well as the order of
        the tests inside each group; the grouping itself is preserved
        and the timing based ordering is skipped.

    :return: A list where each element is a distinct list of test_ids
        (a group), and the union of all the elements is equal to
        set(test_ids).
    """
    # The ids are sorted first: the caller may hand a set of test ids
    # (the test selection returns one), and iterating a set yields a
    # hash order that changes on every run. The worker runs the tests of
    # a group in the order of the group, so a sorted group is a
    # deterministic run order, the same discovery order the non-dynamic
    # runner keeps.
    group_ids = _group_test_ids(sorted(test_ids), group_callback)
    if randomize:
        # Randomize the order in which the groups are picked from the
        # queue, and the order of the tests inside each group, keeping
        # the grouping itself as the unit of work. The timing data
        # based ordering is skipped: the pick-up order is random.
        group_list = list(group_ids.values())
        random.shuffle(group_list)
        for group in group_list:
            random.shuffle(group)
        return group_list
    timed_tests, unknown_tests = _load_time_data(test_ids, repository)
    timed, partial, unknown = _time_groups(group_ids, timed_tests, unknown_tests)
    # The workers pull the groups from the queue, so the scheduling only
    # orders the groups from the slowest to the fastest estimated one:
    # the timed groups first, then the partially timed ones, and the
    # groups without any timed test last.
    dynamic_test_list = []

    def consume_queue(groups):
        queue = sorted(groups.items(), key=operator.itemgetter(1), reverse=True)
        for group_id, _duration in queue:
            dynamic_test_list.append(group_ids[group_id])

    consume_queue(timed)
    consume_queue(partial)
    for group_id in unknown:
        dynamic_test_list.append(group_ids[group_id])

    return dynamic_test_list


def partition_tests(test_ids, concurrency, repository, group_callback, randomize=False):
    """Partition test_ids by concurrency.

    Test durations from the repository are used to get partitions which
    have roughly the same expected runtime. New tests - those with no
    recorded duration - are allocated in round-robin fashion to the
    partitions created using test durations.

    :param list test_ids: The list of test_ids to be partitioned
    :param int concurrency: The concurrency that will be used for running
        the tests. This is the number of partitions that test_ids will be
        split into.
    :param repository: A repository object that
    :param group_callback: A callback function that is used as a scheduler
        hint to group test_ids together and treat them as a single unit for
        scheduling. This function expects a single test_id parameter and it
        will return a group identifier. Tests_ids that have the same group
        identifier will be kept on the same worker.
    :param bool randomize: If true each partition's test order will be
                        randomized

    :return: A list where each element is a distinct subset of test_ids,
        and the union of all the elements is equal to set(test_ids).
    """

    partitions = [list() for i in range(concurrency)]
    timed_partitions = [[0.0, partition] for partition in partitions]
    timed_tests, unknown_tests = _load_time_data(test_ids, repository)
    group_ids = _group_test_ids(test_ids, group_callback)
    timed, partial, unknown = _time_groups(group_ids, timed_tests, unknown_tests)

    # Scheduling is NP complete in general, so we avoid aiming for
    # perfection. A quick approximation that is sufficient for our general
    # needs:
    # sort the groups by time
    # allocate to partitions by putting each group in to the partition with
    # the current (lowest time, shortest length[in tests])
    def consume_queue(groups):
        queue = sorted(groups.items(), key=operator.itemgetter(1), reverse=True)
        for group_id, duration in queue:
            timed_partitions[0][0] = timed_partitions[0][0] + duration
            timed_partitions[0][1].extend(group_ids[group_id])
            timed_partitions.sort(key=lambda item: (item[0], len(item[1])))

    consume_queue(timed)
    consume_queue(partial)
    # Assign groups with entirely unknown times in round robin fashion to
    # the partitions.
    for partition, group_id in zip(itertools.cycle(partitions), unknown):
        partition.extend(group_ids[group_id])
    if randomize:
        out_parts = []
        for partition in partitions:
            temp_part = list(partition)
            random.shuffle(temp_part)
            out_parts.append(list(temp_part))
        return out_parts
    else:
        return partitions


def local_concurrency():
    """Get the number of available CPUs on the system.

    :return: An int for the number of cpus. Or None if it couldn't be found
    """

    try:
        return multiprocessing.cpu_count()
    except NotImplementedError:
        # No concurrency logic known.
        return None


def generate_worker_partitions(
    ids, worker_path, repository=None, group_callback=None, randomize=False
):
    """Parse a worker yaml file and generate test groups

    :param list ids: A list of test ids too be partitioned
    :param path worker_path: The path to a worker file
    :param repository: A repository object that will be used for looking up
        timing data. This is optional, and also will only be used for
        scheduling if there is a count field on a worker.
    :param group_callback: A callback function that is used as a scheduler
        hint to group test_ids together and treat them as a single unit for
        scheduling. This function expects a single test_id parameter and it
        will return a group identifier. Tests_ids that have the same group
        identifier will be kept on the same worker. This is optional and
        also will only be used for scheduling if there is a count field on a
        worker.
    :param bool randomize: If true each partition's test order will be
        randomized. This is optional and also will only be used for scheduling
        if there is a count field on a worker.

    :returns: A list where each element is a distinct subset of test_ids.
    """
    with open(worker_path) as worker_file:
        workers_desc = yaml.safe_load(worker_file.read())
    worker_groups = []
    for worker in workers_desc:
        if isinstance(worker, dict) and "worker" in worker.keys():
            if isinstance(worker["worker"], list):
                local_worker_list = selection.filter_tests(worker["worker"], ids)

                if "concurrency" in worker.keys() and worker["concurrency"] > 1:
                    partitioned_tests = partition_tests(
                        local_worker_list,
                        worker["concurrency"],
                        repository,
                        group_callback,
                        randomize,
                    )
                    worker_groups.extend(partitioned_tests)
                else:
                    # If a worker partition is empty don't add it to the output
                    if local_worker_list:
                        worker_groups.append(local_worker_list)
            else:
                raise TypeError("The input yaml is the incorrect format")
        else:
            raise TypeError("The input yaml is the incorrect format")
    return worker_groups
