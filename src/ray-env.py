"""
Setup a Python remote environment by importing a new Python library
https://docs.ray.io/en/latest/ray-core/handling-dependencies.html

aws s3 ls air-example-data  --no-sign-request
aws s3 cp s3://air-example-data/iris.csv .  --no-sign-request
"""

import logging
import pathlib

import ray

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)

runtime_env = {
    "pip": ["emoji"],
}

ray.init(address="127.0.0.1:6379", runtime_env=runtime_env)


@ray.remote
def do_some_work(x):
    import emoji  # type: ignore

    print(emoji.emojize("Python is :thumbs_up:"))
    return x


def main():
    value = 3
    obj_id = do_some_work.remote(value)
    result = ray.get(obj_id)
    print(f"{result=}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
