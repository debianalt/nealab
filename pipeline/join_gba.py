"""
Post-ingest census-anchored est_personas join for GBA buildings.

Runs ONLY the spatial-join / est_personas stage against an already-populated
gba_buildings_<territory> table (produced by ingest_gba.py). It deliberately
does NOT call import_gba_corrientes.main(), which would re-fetch Overture and
DROP the table — destroying the GBA footprints we just ingested.

Method (unchanged, reused verbatim): volume-proportional (area×height)
dasymetric allocation of the authoritative census total within each census
unit. AR → INDEC radios. (PY distrito DGEEC = Phase 2, added when needed.)

Usage:
  python pipeline/join_gba.py --territory corrientes
"""
from __future__ import annotations

import argparse
import sys

import psycopg2

# Reuse the canonical, proven join logic. import_gba_corrientes defines:
#   TABLE = "gba_buildings_corrientes", PG_BUILDINGS,
#   load_corrientes_radios(), spatial_join_and_est_personas(conn, radios),
#   print_stats(conn)
import import_gba_corrientes as cor

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def join_corrientes() -> None:
    conn = psycopg2.connect(cor.PG_BUILDINGS)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {cor.TABLE}")
            n = cur.fetchone()[0]
        if not n:
            print(f"ERROR: {cor.TABLE} is empty — run ingest_gba.py first", file=sys.stderr)
            sys.exit(1)
        print(f"  {cor.TABLE}: {n:,} GBA buildings (pre-join)")
        radios = cor.load_corrientes_radios()
        print(f"  {len(radios):,} Corrientes radios loaded")
        cor.spatial_join_and_est_personas(conn, radios)
        cor.print_stats(conn)
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Census-anchored est_personas join for GBA buildings")
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()

    if args.territory == "corrientes":
        join_corrientes()
    elif args.territory in ("itapua_py", "alto_parana_py"):
        print(f"NOTE: '{args.territory}' does NOT use join_gba — the PY builder "
              f"build_itapua_buildings.py --source gba does the distrito + DGEEC "
              f"enrichment itself. Run that directly after ingest_gba.")
        return 0
    else:
        print(f"ERROR: territory '{args.territory}' not supported.", file=sys.stderr)
        return 2
    print("\nDONE. Next: build_corrientes_buildings.py (PMTiles, local) "
          "→ compare vs Misiones before R2/deploy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
