"""
Validate a freshly-ingested GBA territory against Misiones (the canonical GBA
reference). The whole point of the GBA migration is consistent *modeled
heights* — so the key checks are the height distribution and the share of
buildings stuck at the 5 m fallback (the Overture-flat symptom), plus a
dasymetric sanity check (sum est_personas ≈ census total).

Usage:
  python pipeline/validate_gba.py --territory corrientes
"""
from __future__ import annotations

import argparse
import sys

import psycopg2

PG_BUILDINGS = "dbname=ndvi_misiones user=postgres"

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_STATS_SQL = """
SELECT
  COUNT(*)                                                AS n,
  ROUND(AVG(best_height_m)::numeric, 2)                   AS avg_h,
  ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
           ORDER BY best_height_m))::numeric, 2)          AS med_h,
  ROUND(MAX(best_height_m)::numeric, 1)                   AS max_h,
  ROUND(100.0 * SUM((best_height_m <= 5.0)::int)
        / NULLIF(COUNT(*), 0), 1)                         AS pct_at_floor,
  ROUND(AVG(area_m2)::numeric, 1)                         AS avg_area,
  COUNT(redcode)                                          AS with_redcode,
  COALESCE(SUM(est_personas)::bigint, 0)                  AS sum_est_pers
FROM {table}
"""


def stats(cur, table: str) -> dict:
    cur.execute(f"SELECT to_regclass('{table}')")
    if cur.fetchone()[0] is None:
        return {}
    cur.execute(_STATS_SQL.format(table=table))
    cols = ["n", "avg_h", "med_h", "max_h", "pct_at_floor",
            "avg_area", "with_redcode", "sum_est_pers"]
    return dict(zip(cols, cur.fetchone()))


def fmt(d: dict) -> str:
    if not d:
        return "  (table missing)"
    return (f"  n={d['n']:,}  avg_h={d['avg_h']}m  med_h={d['med_h']}m  "
            f"max_h={d['max_h']}m  %at_5m_floor={d['pct_at_floor']}%\n"
            f"  avg_area={d['avg_area']}m2  with_redcode={d['with_redcode']:,}  "
            f"sum_est_personas={d['sum_est_pers']:,}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    table = f"gba_buildings_{args.territory}"

    conn = psycopg2.connect(PG_BUILDINGS)
    try:
        with conn.cursor() as cur:
            ref = stats(cur, "gba_buildings")          # Misiones canonical
            new = stats(cur, table)                    # ingested territory
    finally:
        conn.close()

    print("=" * 60)
    print(f"GBA VALIDATION — {args.territory} vs Misiones (canonical)")
    print("=" * 60)
    print("\nMisiones (gba_buildings):")
    print(fmt(ref))
    print(f"\n{args.territory} ({table}):")
    print(fmt(new))

    if not new:
        print("\nFAIL: ingested table missing — run ingest_gba.py.")
        return 1

    # Heuristic verdict: a healthy GBA territory should NOT be mostly stuck at
    # the 5 m floor (that is exactly the Overture-flat symptom we are fixing),
    # and its median height should be in a realistic urban/rural range.
    flat = new["pct_at_floor"] is not None and new["pct_at_floor"] > 60
    med = float(new["med_h"]) if new["med_h"] is not None else 0
    realistic = 2.0 <= med <= 30.0
    print("\nVerdict:")
    print(f"  height not Overture-flat (%at_5m<=60): {'OK' if not flat else 'CHECK'}")
    print(f"  median height realistic (2-30m):       {'OK' if realistic else 'CHECK'}")
    print(f"  redcode/est_personas assigned:         "
          f"{'OK' if new['with_redcode'] else 'MISSING — run join_gba.py'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
