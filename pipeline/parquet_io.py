"""Shared writer for H3 composite parquets served to the browser.

DuckDB-WASM reads these over httpfs range requests. Writing them SNAPPY /
unsorted / single-row-group (the pandas default) bloats the always-projected
h3index column (~2.2 MB, ~32% of a composite) and prevents row-group skipping.
Writing them ZSTD + sorted by h3index + fixed row groups collapses the shared
string prefixes (h3index ~2.2 MB -> ~66 KB) and lets DuckDB skip groups.

Same encoding as the optimize_parquets.py backfill, applied at write time so
future pipeline runs / new territories ship already-slim. Lossless: sort keeps
every row, ZSTD is a lossless codec.
"""
from __future__ import annotations

import pandas as pd


def write_h3_parquet(
    df: pd.DataFrame,
    path,
    *,
    sort_col: str = "h3index",
    compression: str = "zstd",
    compression_level: int = 9,
    row_group_size: int = 50000,
    index: bool = False,
    **kwargs,
) -> None:
    """Write ``df`` as a ZSTD, h3index-sorted, row-grouped parquet.

    Falls back to writing unsorted if ``sort_col`` is absent (never raises on a
    missing column). Extra kwargs pass through to pandas/pyarrow.
    """
    if sort_col in df.columns:
        df = df.sort_values(sort_col)
    df.to_parquet(
        path,
        index=index,
        compression=compression,
        compression_level=compression_level,
        row_group_size=row_group_size,
        **kwargs,
    )
