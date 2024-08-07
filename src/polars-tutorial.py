"""
https://docs.pola.rs/
"""

import logging
import pathlib
import datetime as dt

import numpy as np
import polars as pl


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def basics():
    df = pl.DataFrame(
        {
            "integer": [1, 2, 3],
            "date": [
                dt.datetime(2025, 1, 1),
                dt.datetime(2025, 1, 2),
                dt.datetime(2025, 1, 3),
            ],
            "float": [4.0, 5.0, 6.0],
            "string": ["a", "b", "c"],
        }
    )
    print(df)
    df = pl.DataFrame(
        {
            "a": range(5),
            "b": [0.952078, 0.625541, 0.471593, 0.023061, 0.970908],
            "c": [
                "2025-12-01",
                "2025-12-02",
                "2025-12-03",
                "2025-12-04",
                "2025-12-05",
            ],
            "d": [1.0, 2.0, np.nan, -42.0, None],
        }
    )
    df = df.with_columns(pl.col("c").str.to_datetime("%Y-%m-%d"))
    print(df)

    print(df.select(pl.col("a", "b")))

    print(
        df.filter(
            pl.col("c").is_between(
                dt.datetime(2025, 12, 2), dt.datetime(2025, 12, 3)
            ),
        )
    )
    print(df.filter((pl.col("a") <= 3) & (pl.col("d").is_not_nan())))
    print(
        df.with_columns(
            pl.col("b").sum().alias("e"), (pl.col("b") + 42).alias("b+42")
        )
    )

    df2 = pl.DataFrame(
        {
            "x": range(8),
            "y": ["A", "A", "A", "B", "B", "C", "X", "X"],
        }
    )
    print(df2)

    print(df2.group_by("y", maintain_order=True).len())

    print(
        df2.group_by("y", maintain_order=True).agg(
            pl.col("*").count().alias("count"),
            pl.col("*").sum().alias("sum"),
        )
    )

    df_x = df.with_columns((pl.col("a") * pl.col("b")).alias("a * b")).select(
        pl.all().exclude(["c", "d"])
    )
    print(df_x)

    df = pl.DataFrame(
        {
            "a": range(8),
            "b": np.random.rand(8),
            "d": [
                1.0,
                2.0,
                float("nan"),
                float("nan"),
                0.0,
                -5.0,
                -42.0,
                None,
            ],
        }
    )

    df2 = pl.DataFrame(
        {
            "x": range(8),
            "y": ["A", "A", "A", "B", "B", "C", "X", "X"],
        }
    )
    joined = df.join(df2, left_on="a", right_on="x")
    print(joined)

    stacked = df.hstack(df2)
    print(stacked)


def main():
    pl.show_versions()
    basics()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    # logging.basicConfig(level=logging.DEBUG)
    main()
