"""
"""

import logging
import pathlib
import tempfile
import time
from functools import wraps

import duckdb
import polars as pl

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


DATA_DIR = pathlib.Path("~/ws/df-lib/data").expanduser()


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        # print(
        #     "func:%r args:[%r, %r] took: %2.4f sec"
        #     % (f.__name__, args, kw, te - ts)
        # )
        print("func:%r took: %0.4f sec" % (f.__name__, te - ts))
        return result

    return wrap


def get_data_file():
    """
        id1 - varchar
        id2 - varchar
        id3 - varchar
        id4 - int64
        id5 - int64
        id6 - int64
        v1  - int64
        v2  - int64
        v3  - double

    ┌───────┬───────┬──────────────┬─────┬─────┬───────────┬─────┬────┬───────────┐
    │ id1   │ id2   │     id3      │ id4 │ id5 │    id6    │ v1  │ v2 │    v3     │
    ├───────┼───────┼──────────────┼─────┼─────┼───────────┼─────┼────┼───────────┤
    │ id001 │ id001 │ id0114412362 │ 2   │ 1   │ 173907782 │ 5   │ 12 │  20.28182 │
    │ id002 │ id001 │ id0209308137 │ 1   │ 2   │ 114440553 │ 5   │  6 │ 73.316986 │
    └───────┴───────┴──────────────┴─────┴─────┴───────────┴─────┴────┴───────────┘
    """
    data_file = DATA_DIR / "G1_1e9_2e0_0_0.csv.zst"
    return data_file


@timing
def timed_sql(sql):
    print(duckdb.sql(sql).show())


def set_duckdb_temp_dir():
    temp_dir = pathlib.Path(tempfile.gettempdir()) / ".duckdb_temp_dir"
    duckdb.sql("set temp_directory='{}'".format(temp_dir))


@timing
def billion_median(df, col):
    print(df.select(pl.col(col).median()).collect())


@timing
def billion_avg(df, col):
    print(df.select(pl.col(col).mean()).collect())


@timing
def billion_sample_median(df, col):
    print(df.select(pl.col(col).sample(fraction=0.01).median()).collect())


@timing
def billion_sample_avg(df, col):
    print(df.select(pl.col(col).sample(fraction=0.01).mean()).collect())


def main():
    pq_file = DATA_DIR / "G1_1e9_2e0_0_0-zstd.parquet"
    thread_pool_size = pl.thread_pool_size()
    print(f"{thread_pool_size=}")
    df = pl.scan_parquet(pq_file)

    billion_median(df, "v2")
    billion_sample_median(df, "v2")

    billion_avg(df, "v2")
    billion_sample_avg(df, "v2")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
