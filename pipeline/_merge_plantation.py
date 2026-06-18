"""Combine current-state + 2020-baseline plantation parquets into the served
eudr_plantation_res9.parquet (h3index, plantation_pct, native_forest_pct,
plantation_2020_pct, native_2020_pct, province). LEFT JOIN on 2020 so it works
even before the 2020 baseline exists (those columns stay null)."""
import os
import duckdb

H = "pipeline/output/eudr/hires"
CUR = H + "/plantation_ar_*_res9.parquet"        # current state (2022/23)
B20 = H + "/plantation2020_ar_*_res9.parquet"    # 2020 baseline
OUT = H + "/eudr_plantation_res9.parquet"

con = duckdb.connect()
con.execute("SET enable_progress_bar=false;")

import glob
has2020 = bool(glob.glob(B20))
join_2020 = f"""
    LEFT JOIN (
        SELECT * EXCLUDE(rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY h3index ORDER BY mb_px DESC) rn
            FROM read_parquet('{B20}')
        ) WHERE rn = 1
    ) b USING (h3index)""" if has2020 else ""
sel_2020 = ("b.plantation_pct AS plantation_2020_pct, b.native_forest_pct AS native_2020_pct"
            if has2020 else "CAST(NULL AS DOUBLE) AS plantation_2020_pct, CAST(NULL AS DOUBLE) AS native_2020_pct")

con.execute(f"""
COPY (
    SELECT c.h3index, c.plantation_pct, c.native_forest_pct,
           {sel_2020}, c.province
    FROM (
        SELECT * EXCLUDE(rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY h3index ORDER BY mb_px DESC) rn
            FROM read_parquet('{CUR}')
        ) WHERE rn = 1
    ) c{join_2020}
    ORDER BY c.h3index
) TO '{OUT}' (FORMAT parquet, ROW_GROUP_SIZE 50000)
""")

r = con.execute(f"""SELECT province, count(*) n,
    round(avg(plantation_pct),1) cur,
    round(avg(plantation_2020_pct),1) p2020,
    count(*) FILTER (WHERE plantation_2020_pct IS NOT NULL) has2020
    FROM read_parquet('{OUT}') GROUP BY 1 ORDER BY 2 DESC""").fetchall()
print(f"has_2020_baseline={has2020}")
for x in r:
    print(f"  {x[0]}: {x[1]:,} cells  cur_plant={x[2]}  plant2020={x[3]}  with2020={x[4]:,}")
print(f"size {os.path.getsize(OUT)/1024/1024:.1f} MB -> {OUT}")
