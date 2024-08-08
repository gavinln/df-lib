"""
Before running this code run: make ray-head-start
https://docs.ray.io/en/latest/ray-core/walkthrough.html
"""

import logging
import pathlib

import fire
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


def main():
    fire.Fire({"ray-task": ray_task, "ray-actor": ray_actor})


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
