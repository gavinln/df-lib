"""
https://duckdb.org/docs/api/python/overview
"""

import logging
import pathlib
import tempfile
import datetime as dt

import duckdb

import pandas as pd
import polars as pl
import pyarrow as pa


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def copy_to_file_sql(sql, file_name, format):
    formats = ["parquet", "json", "csv"]
    assert (
        format in formats
    ), f"Invalid format {format}. Should be one of {formats}"
    return f"""
        COPY
            ({sql})
        TO
            '{file_name}'
            (FORMAT '{format}')
    """


def print_file_info(file_path):
    stat = file_path.stat()
    print(
        "file {}\nsize {}\nmodified {}".format(
            file_path,
            stat.st_size,
            dt.datetime.fromtimestamp(stat.st_mtime).strftime("%F %T"),
        )
    )


def basics():
    # in-memory database that returns a relation
    rel = duckdb.sql("SELECT 42 as i")
    print(rel)

    # access relations as tables
    duckdb.sql("select i * 2 as k from rel").show()

    temp_dir = pathlib.Path(tempfile.gettempdir())
    # temp_parquet = temp_dir / "example.parquet"

    sql_data = """
        select 'Alice' as name, 42 as age
        union all
        select 'Bob' as name, 32 as age
    """
    with tempfile.TemporaryDirectory() as temp_dir:

        temp_csv = pathlib.Path(temp_dir) / "example.csv"
        csv_sql = copy_to_file_sql(sql_data, temp_csv, "csv")
        duckdb.sql(csv_sql)
        print(temp_csv)
        print(temp_csv.read_text())
        # read from csv
        print(duckdb.sql("select * from '{}'".format(temp_csv)))

        temp_json = pathlib.Path(temp_dir) / "example.json"
        json_sql = copy_to_file_sql(sql_data, temp_json, "json")
        duckdb.sql(json_sql)
        print(temp_json)
        print(temp_json.read_text())
        # read from json
        print(duckdb.sql("select * from '{}'".format(temp_json)))

        temp_parquet = pathlib.Path(temp_dir) / "example.parquet"
        print(f"{temp_parquet} exists: {temp_parquet.exists()}")
        duckdb.sql(sql_data).write_parquet(str(temp_parquet))
        print(f"{temp_parquet} exists: {temp_parquet.exists()}")

        print_file_info(temp_parquet)

        rel = duckdb.read_parquet(str(temp_parquet))
        duckdb.sql("select * from rel").show()

    # select from pandas, polars, arrow
    pandas_df = pd.DataFrame({"a": [42]})
    _ = pandas_df
    duckdb.sql("select a as pandas_col from pandas_df").show()

    polars_df = pl.DataFrame({"a": [42]})
    _ = polars_df
    duckdb.sql("select a as polars_col from polars_df").show()

    arrow_table = pa.Table.from_pydict({"a": [42]})
    _ = arrow_table
    duckdb.sql("select a as arrow_col from arrow_table").show()

    # convert result to python objects, pandas, polars, arrow, numpy
    print(duckdb.sql("select 42").fetchall())
    print(duckdb.sql("select 42 as pandas_col").df())
    print(duckdb.sql("select 42 as polars_col").pl())
    print(duckdb.sql("select 42 as arrow_col").arrow())
    print(duckdb.sql("select 42 as numpy_col").fetchnumpy())

    # in-memory database
    con = duckdb.connect()
    con.sql("select 42 as x").show()

    # persistent storage
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_db = pathlib.Path(temp_dir) / "example.db"

        # new database
        con = duckdb.connect(str(temp_db))
        con.sql("create table test (i integer)")
        con.sql("insert into test values (42)")
        con.table("test").show()
        con.close()

        print_file_info(temp_db)

        # update database, closed automatically
        with duckdb.connect(str(temp_db)) as con:
            con.sql("insert into test values (42)")
            con.table("test").show()

    # register dataframes and arrow objects
    my_dict = {}
    my_dict["test_df"] = pd.DataFrame.from_dict(
        {"i": [1, 2], "j": ["one", "two"]}
    )
    duckdb.register("test_df_view", my_dict["test_df"])
    print(duckdb.sql("SELECT * FROM test_df_view").fetchall())


def db_api():
    # default database
    duckdb.execute("create table tbl as select 42 a")
    con = duckdb.connect(":default:")
    con.sql("select * from tbl").show()

    con.execute(
        "CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)"
    )
    con.execute(
        "INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)"
    )

    con.execute("SELECT * FROM items")
    print(con.fetchall())
    # [('jeans', Decimal('20.00'), 1), ('hammer', Decimal('42.20'), 2)]

    # retrieve the items one at a time
    con.execute("SELECT * FROM items")
    print(con.fetchone())

    con.execute("INSERT INTO items VALUES (?, ?, ?)", ["laptop", 2000, 1])
    con.execute("SELECT * FROM items")
    for item in con.fetchall():
        print(item)


def relational_api():
    rel = duckdb.sql("SELECT * FROM range(1_000_000) tbl(id)")
    duckdb.sql("SELECT sum(id) FROM rel").show()
    rel.aggregate("id % 2 as g, sum(id), min(id), max(id)").show()

    r1 = duckdb.sql("select * from range(10) tbl(id)").set_alias("r1")
    r2 = duckdb.sql("select * from range(5) tbl(id)").set_alias("r2")
    r1.except_(r2).show()

    r1.filter("id > 6").limit(2).show()
    
    r1.intersect(r2).show()

    r1.join(r2, "r1.id - 5 = r2.id").show()

    r1.order("id desc").limit(2).show()

    r1.project("id + 10 as id_plus_ten").limit(2).show()

    r1.union(r2).show()


def main():
    # basics()
    # db_api()
    relational_api()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
