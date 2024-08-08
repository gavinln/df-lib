"""
https://rise.cs.berkeley.edu/blog/ray-tips-for-first-time-users/
"""

import logging
import pathlib
import random
import time

import numpy as np
import ray

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)

ray.init(address="127.0.0.1:6379")


@ray.remote
def square(x):
    return x * x


def ray_task():
    "run tasks on ray"
    futures = [square.remote(i) for i in range(4)]

    # Retrieve results.
    print(ray.get(futures))


@ray.remote
class Counter:
    def __init__(self):
        self.i = 0

    def get(self):
        return self.i

    def incr(self, value):
        self.i += value


def ray_actor():
    c = Counter.remote()

    for _ in range(10):
        c.incr.remote(1)  # type: ignore

    # Retrieve final actor state.
    print(ray.get(c.get.remote()))  # type: ignore


def do_some_work1(x):
    time.sleep(1)
    return x


def no_ray_job():
    "execute local jobs sequentially"
    start = time.time()
    results = [do_some_work1(x) for x in range(4)]
    print("no-ray duration =", time.time() - start, "\nresults = ", results)


@ray.remote
def do_some_work2(x):
    time.sleep(1)
    return x


def ray_job():
    "execute multiple remote jobs in parallel"
    start = time.time()
    results = [do_some_work2.remote(x) for x in range(4)]
    print(ray.get(results))
    print("ray duration =", time.time() - start, "\nresults = ", results)


def tiny_work1(x):
    time.sleep(0.0001)
    return x


def local_tiny_work():
    "run multiple tiny work functions individually"
    start = time.time()
    _ = [tiny_work1(x) for x in range(100000)]
    print("local tiny work duration =", time.time() - start)


@ray.remote
def tiny_work2(x):
    time.sleep(0.0001)
    return x


def remote_tiny_work():
    "execute multiple tiny work function individually"
    start = time.time()
    _ = [tiny_work2.remote(x) for x in range(100000)]
    print("remote tiny work duration =", time.time() - start)


@ray.remote
def batched_tiny_work(start, end):
    return [tiny_work1(x) for x in range(start, end)]


def remote_batched_tiny_work():
    "batch multiple tiny work functions"
    start = time.time()
    result_ids = []
    [
        result_ids.append(batched_tiny_work.remote(x * 1000, (x + 1) * 1000))
        for x in range(100)
    ]
    _ = ray.get(result_ids)
    print("remote batched tiny work duration =", time.time() - start)


@ray.remote
def no_work(x):
    return x


def remote_no_work():
    "measure overhead of an empty function"
    start = time.time()
    num_calls = 1000
    [ray.get(no_work.remote(x)) for x in range(num_calls)]
    print("per task overhead (ms) =", (time.time() - start) * 1000 / num_calls)


def remote_large_object_repeat():
    "pass a large object multiple times"
    start = time.time()
    a = np.zeros((10000, 2000))
    result_ids = [no_work.remote(a) for _ in range(10)]
    _ = ray.get(result_ids)
    print("remote large object repeat duration =", time.time() - start)


def remote_large_object_once():
    "pass a large object once and use object id"
    start = time.time()
    a = np.zeros((10000, 2000))
    a_id = ray.put(a)
    result_ids = [no_work.remote(a_id) for _ in range(10)]
    _ = ray.get(result_ids)
    print("remote large object once duration =", time.time() - start)


@ray.remote
def random_time_task(x):
    time.sleep(random.uniform(0, 4))
    return x


def process_sequential(results):
    sum = 0
    for result in results:
        time.sleep(1)
        sum += result
    return sum


def remote_wait_for_all():
    start = time.time()
    result_ids = [random_time_task.remote(i) for i in range(4)]
    results = ray.get(result_ids)
    print("result = {}".format(process_sequential(results)))
    print("remote wait for all duration =", time.time() - start)


def process_next(result_ids):
    sum = 0
    while len(result_ids):
        done_id, result_ids = ray.wait(result_ids)
        result = ray.get(done_id[0])
        time.sleep(1)
        sum += result
    return sum


def remote_wait_for_next():
    start = time.time()
    result_ids = [random_time_task.remote(i) for i in range(4)]
    print("result = {}".format(process_next(result_ids)))
    print("remote wait for next duration =", time.time() - start)


def main():
    # no_ray_job()
    # ray_job()

    # local_tiny_work()
    # remote_tiny_work()
    # remote_batched_tiny_work()

    # remote_no_work()

    # remote_large_object_repeat()
    # remote_large_object_once()

    remote_wait_for_all()
    remote_wait_for_next()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
