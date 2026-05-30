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
import os
import sys

import psycopg2

# Reuse the canonical, proven join logic. import_gba_corrientes defines:
#   TABLE = "gba_buildings_corrientes", PG_BUILDINGS,
#   load_radios(codprov), spatial_join_and_est_personas(conn, radios, table),
#   print_stats(conn, table)
import import_gba_corrientes as cor

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import get_territory

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# AR provinces with INDEC radios censales 2022. codprov from config.TERRITORY_CONFIGS.
AR_CODPROV = {"corrientes": "18", "chaco": "22", "formosa": "34"}


def join_ar(territory: str) -> None:
    """Census-anchored volume-proportional est_personas join for any AR province.
    Generalizes the proven Corrientes path to gba_buildings_<territory> +
    INDEC radios of the province's codprov (same method as Misiones)."""
    table = f"gba_buildings_{territory}"
    cfg = get_territory(territory)
    codprov = str(cfg.get("codprov_indec") or AR_CODPROV.get(territory) or "")
    if not codprov:
        print(f"ERROR: no codprov_indec for '{territory}' in config", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(cor.PG_BUILDINGS)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            n = cur.fetchone()[0]
        if not n:
            print(f"ERROR: {table} is empty — run ingest_gba.py --territory {territory} first",
                  file=sys.stderr)
            sys.exit(1)
        print(f"  {table}: {n:,} GBA buildings (pre-join)")
        radios = cor.load_radios(codprov)
        print(f"  {len(radios):,} {territory} radios loaded (codprov {codprov})")
        cor.spatial_join_and_est_personas(conn, radios, table=table)
        cor.print_stats(conn, table=table)
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Census-anchored est_personas join for GBA buildings")
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()

    if args.territory in AR_CODPROV:
        join_ar(args.territory)
    elif args.territory in ("itapua_py", "alto_parana_py"):
        print(f"NOTE: '{args.territory}' does NOT use join_gba — the PY builder "
              f"build_itapua_buildings.py --source gba does the distrito + DGEEC "
              f"enrichment itself. Run that directly after ingest_gba.")
        return 0
    else:
        print(f"ERROR: territory '{args.territory}' not supported.", file=sys.stderr)
        return 2
    print(f"\nDONE. Next: build_ar_buildings.py --territory {args.territory} (PMTiles, local) "
          "→ compare vs Misiones before R2/deploy.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
