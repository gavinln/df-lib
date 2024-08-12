"""
https://docs.ray.io/en/latest/data/data.html

# data examples are stored on this Amazon S3 bucket

aws s3 ls air-example-data  --no-sign-request
aws s3 cp s3://air-example-data/iris.csv .  --no-sign-request
"""

import logging
import os
import pathlib
from typing import Any

import fire
import ray

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


ray.init(address="127.0.0.1:6379")


@ray.remote
def do_some_work(x):
    import emoji  # type: ignore

    print(emoji.emojize("Python is :thumbs_up:"))
    return x


def load_parquet():
    print("load parquet", "-" * 10)
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    print(ds)
    print("schema", "-" * 10)
    print(ds.schema())
    print("columns", "-" * 10)
    print(ds.columns())
    print("count", "-" * 10)
    print(ds.count())


def load_python_objects():
    print("load python objects", "-" * 10)
    ds = ray.data.from_items(
        [
            {"food": "spam", "price": 9.34},
            {"food": "ham", "price": 5.37},
            {"food": "eggs", "price": 0.94},
        ]
    )
    print(ds)


def load_synthetic_data():
    print("load synthetic data", "-" * 10)
    ds = ray.data.range(10000)
    print(ds)
    ds = ray.data.range_tensor(10, shape=(64, 64))
    print(ds)
    ds2 = ds.materialize()
    print("num_blocks:", ds2.num_blocks())
    print("size_bytes:", ds2.size_bytes())


def ray_data_load():
    "load data with ray"
    load_parquet()
    load_python_objects()
    load_synthetic_data()


def ray_data_inspect():
    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    rows = ds.take(2)
    for idx, row in enumerate(rows):
        print(idx, "-" * 10, row)

    batch = ds.take_batch(batch_size=2)
    print(batch)


def parse_filename(row: dict[str, Any]) -> dict[str, Any]:
    row["filename"] = os.path.basename(row["path"])
    return row


def ray_data_transform():
    ds = ray.data.read_images(
        "s3://anonymous@ray-example-data/image-datasets/simple",
        include_paths=True,
    ).map(parse_filename)
    print(ds.schema())
    ds2 = ds.select_columns(["path", "filename"])
    df = ds2.to_pandas()
    print(df)


def main():
    fire.Fire(
        {
            "ray-data-load": ray_data_load,
            "ray-data-inspect": ray_data_inspect,
            "ray-data-transform": ray_data_transform,
        }
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
