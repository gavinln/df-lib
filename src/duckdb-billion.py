"""


"""

import logging
import pathlib
import pprint as pp

from functools import wraps
from time import time

import duckdb
import tempfile


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


DATA_DIR = pathlib.Path("~/ws/df-lib/data").expanduser()


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
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


def copy_csv_to_parquet(csv_file, parquet_file):
    csv_to_parquet = f"""
        COPY (
            select * from read_csv('{csv_file}')
        )
        TO '{parquet_file}'
            (FORMAT 'parquet', CODEC 'zstd')
    """
    duckdb.sql(csv_to_parquet)


def get_parquet_file_metadata(parquet_file) -> dict:
    match parquet_file:
        case pathlib.Path() as p:
            assert p.exists(), f"Parquet file {parquet_file} does not exist"
        case str() as p:
            pq_file = pathlib.Path(parquet_file)
            assert (
                pq_file.exists()
            ), f"Parquet file {parquet_file} does not exist"
        case _:
            print("Unknown type")

    """
    if isinstance(parquet_file, pathlib.Path):
        assert parquet_file.exists(), f"Parquet file {parquet_file} does not exist"
    elif isinstance(parquet_file, str):
        pq_file = pathlib.Path(parquet_file)
        assert pq_file.exists(), f"Parquet file {parquet_file} does not exist"
    """

    sql = f"""
        SELECT *
        FROM parquet_file_metadata('{parquet_file}');
    """
    rel = duckdb.sql(sql)
    row, _ = rel.shape
    assert row == 1, "There should only be one row"
    pq_file_meta = dict(zip(rel.columns, rel.fetchone()))  # type: ignore
    return pq_file_meta


def column_name_type(rel: duckdb.DuckDBPyRelation) -> dict[str, str]:
    return {name: str(val) for (name, val) in zip(rel.columns, rel.dtypes)}


def count_distinct_table(table):
    rel = duckdb.sql(f"select * from {table} limit 0")
    col_names = rel.columns
    col_types = rel.dtypes
    print("count distinct {}".format("-" * 9))
    for col in col_names:
        result = duckdb.sql(
            f"select count(distinct {col}) from {table}"
        ).fetchall()
        print(col, ":", result[0][0])
    print("avg columns {}".format("-" * 9))
    numeric_columns = (
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "DOUBLE",
        "HUGEINT",
        "UHUGEINT",
        "DECIMAL",
    )
    for col, dtype in zip(col_names, col_types):
        if dtype in numeric_columns:
            sql = f"select avg({col}) from {table}"
            result = duckdb.sql(sql).fetchall()
            print(col, ":", result[0][0])


def print_table_stats(table):
    rel = duckdb.sql("select * from {}".format(table))
    pp.pprint(column_name_type(rel))

    rel.describe().show(max_col_width=12)
    """ count distinct from 10 million
        id1 2
        id2 2
        id3 9,901,037
        id4 2
        id5 2
        id6 9,901,176
        v1 5
        v2 15
        v3 8,149,166
    """
    count_distinct_table(table)


@timing
def timed_sql(sql):
    print(duckdb.sql(sql).show())


def set_duckdb_temp_dir():
    temp_dir = pathlib.Path(tempfile.gettempdir()) / ".duckdb_temp_dir"
    duckdb.sql("set temp_directory='{}'".format(temp_dir))


@timing
def billion_median(con, pq_file, column):
    con.sql("select median({}) from '{}'".format(column, pq_file)).show()


@timing
def billion_avg(con, pq_file, column):
    con.sql("select avg({}) from '{}'".format(column, pq_file)).show()


@timing
def billion_approx_median(con, pq_file, column):
    con.sql(
        "select approx_quantile({}, 0.5) from '{}'".format(column, pq_file)
    ).show()


@timing
def billion_sample_median(con, pq_file, column):
    con.sql(
        "select median({}) from '{}' using sample 1%".format(column, pq_file)
    ).show()

@timing
def billion_sample_avg(con, pq_file, column):
    con.sql(
        "select avg({}) from '{}' using sample 1%".format(column, pq_file)
    ).show()


def create_db_from_file(db_file, data_file, table_name):
    "create database from file"

    "Takes about 10 minutes actual time using 6 threads"

    with duckdb.connect(database=str(db_file), read_only=False) as con:
        con.execute(
            "create table {} as select * from '{}'".format(
                table_name, data_file
            )
        )


def main():
    pq_file = DATA_DIR / "G1_1e9_2e0_0_0-zstd.parquet"

    parquet_file_meta = get_parquet_file_metadata(pq_file)
    pp.pprint(parquet_file_meta)

    duckdb.sql("describe table '{}'".format(pq_file)).show()
    duckdb.sql(
        "create table tbl_1000 as select * from '{}' limit 1000".format(
            pq_file
        )
    )
    print_table_stats("tbl_1000")

    with duckdb.connect(":default:") as con:
        billion_median(con, pq_file, "v2")  # 24 seconds
        billion_approx_median(con, pq_file, "v2")  # 20 seconds
        billion_sample_median(con, pq_file, "v2")  # 6 seconds
        billion_avg(con, pq_file, "v3")  # 7 seconds
        billion_sample_avg(con, pq_file, "v3")  # 4 seconds

    # df = duckdb.sql("select * from duckdb_settings()").df()
    # print(df)

    table_name = "tbl"
    temp_db = DATA_DIR / "temp-billion.db"
    # create_db_from_file(temp_db, pq_file, "tbl")
    with duckdb.connect(database=str(temp_db), read_only=False) as con:
        billion_median(con, table_name, "v2")  # 23 seconds
        billion_approx_median(con, table_name, "v2")  # 15 seconds
        billion_sample_median(con, table_name, "v2")  # 1 second
        billion_avg(con, table_name, "v3")  # 5 seconds
        billion_sample_avg(con, table_name, "v3")  # 1 second


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
