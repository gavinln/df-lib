"""
Filed a github ticket using code in this file
https://github.com/pola-rs/polars/issues/18068
"""

import pathlib
import time

import duckdb
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

pq_file = pathlib.Path("polars_example.parquet")
col_name = "value"

# create a 790 mb file
if not pq_file.exists():
    rng = np.random.default_rng()
    names = [col_name]
    data = rng.random(1, dtype=np.float64)
    table = pa.Table.from_arrays([data], names=names)
    with pq.ParquetWriter(pq_file, table.schema) as writer:
        data = rng.random(10_000_000, dtype=np.float64)
        table = pa.Table.from_arrays([data], names=names)
        for _ in range(10):
            writer.write_table(table)

df = pl.scan_parquet(pq_file)

start = time.time()
print(df.select(pl.col(col_name).median()).collect())
print(f"polars median time:\t {time.time() - start:.3f}")  # 14s

start = time.time()
print(df.select(pl.col(col_name).sample(fraction=0.01).median()).collect())
print(f"polars sample median time: {time.time() - start:.3f}")  # 13s

start = time.time()
duckdb.sql(
    "select median({}) from '{}' using sample 1%".format(col_name, pq_file)
).show()
print(f"duckdb sample median time: {time.time() - start:.3f}")  # 5s
