"""
Combine per-region EUDR parquets into the production-served files.

- hires/eudr_*_res9.parquet (AR/PY/BR by region) -> hires/eudr_res9_combined.parquet
  Sorted by h3index, row-groups of 50K for httpfs range-read pruning.
- hires/eudr_*_res7.parquet (AR/PY/BR by region) -> eudr_deforestation.parquet
  (the layer used by the main app at res-7).

Dedup by h3index (cells near borders may appear in multiple region parquets).
"""

import os
import sys
import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_eudr import OUTPUT_DIR

HIRES = os.path.join(OUTPUT_DIR, "hires")


def combine(pattern: str, out_path: str, row_group_size: int = 0):
    con = duckdb.connect()
    rg_clause = f", ROW_GROUP_SIZE {row_group_size}" if row_group_size else ""
    tmp = f"{out_path}.tmp"
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE(rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY h3index ORDER BY province) AS rn
                FROM read_parquet('{pattern}')
            ) WHERE rn = 1
            ORDER BY h3index
        ) TO '{tmp}' (FORMAT parquet{rg_clause})
    """)
    os.replace(tmp, out_path)
    n, prov = con.execute(f"SELECT count(*), count(DISTINCT province) FROM read_parquet('{out_path}')").fetchone()
    print(f"  {os.path.basename(out_path)}: {n:,} rows, {prov} units, "
          f"{os.path.getsize(out_path) / 1024 / 1024:.1f} MB")


def main():
    res9_pattern = os.path.join(HIRES, "eudr_*_res9.parquet").replace("\\", "/")
    res7_pattern = os.path.join(HIRES, "eudr_*_res7.parquet").replace("\\", "/")
    combine(res9_pattern, os.path.join(HIRES, "eudr_res9_combined.parquet"), row_group_size=50000)
    combine(res7_pattern, os.path.join(OUTPUT_DIR, "eudr_deforestation.parquet"))


if __name__ == "__main__":
    main()
