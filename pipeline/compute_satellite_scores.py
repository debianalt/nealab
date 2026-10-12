"""
Compute the census composite scores at H3 res-9 for Spatia.

Methodology: PCA-validated geometric mean (OECD Handbook / UNDP HDI).
  1. Reads radio-level census data, projects onto H3 via the dasymetric crosswalk
  2. Normalizes each component via percentile rank (0-100) or fixed goalposts
  3. Runs PCA diagnostics (correlation matrix, KMO, Bartlett's, varimax)
  4. Selects variables by dropping redundant pairs (|r| > 0.70)
  5. Computes the geometric mean of retained components (floor=1.0, HDI-style).
     All retained components weigh the same: there are no per-component weights.

Analyses (census, served by spatia.ar):
  service_deprivation  — NBI, sewage, floor, overcrowding, computer access
  health_access        — Healthcare time, coverage, NBI, age structure, density
  education_capital    — No schooling, secondary, tertiary, university
  education_flow       — School non-attendance 6-12 / 13-18, teen pregnancy

Raster-based analyses (agri_potential) run in process_raster_to_h3.py.

Usage:
  python pipeline/compute_satellite_scores.py
  python pipeline/compute_satellite_scores.py --only health_access --territory corrientes
  python pipeline/compute_satellite_scores.py --diagnostics
"""

import argparse
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

from config import OUTPUT_DIR
from parquet_io import write_h3_parquet
from scoring import (
    geometric_mean_score,
    run_full_diagnostics,
    generate_report,
    load_goalposts,
    score_with_goalposts,
)

CROSSWALK_PATH = os.path.join(OUTPUT_DIR, "h3_radio_crosswalk.parquet")
RADIO_DATA_DIR = os.path.join(OUTPUT_DIR, "radio_data")

# Tables used by analysis queries — loaded from parquets
RADIO_TABLES = ["radios_misiones", "oxford_accessibility", "censo2022_variables"]

# ── Analysis definitions ─────────────────────────────────────────────────────
# Each analysis: id, sql (returns redcode + component columns), components
# Components: (output_col, sql_col, invert)
#   invert=True means lower raw value = higher score

ANALYSIS_DEFS = [
    {
        "id": "service_deprivation",
        "sql": """
            SELECT r.redcode,
                COALESCE(ce.pct_nbi, 0) AS c_nbi,
                -- pct_cloacas YA es "% de hogares SIN cloaca" (censo_nbi_2022.h_cloaca =
                -- "Hogares sin cloaca"; corr +0.10 con NBI). NO invertir: el 100- previo
                -- la daba vuelta a "% con cloaca" y la rankeaba como carencia al revés.
                COALESCE(ce.pct_cloacas, 0) AS c_sin_cloacas,
                COALESCE(ce.pct_sin_piso_adecuado, 0) AS c_piso,
                COALESCE(ce.pct_hacinamiento, 0) AS c_hacinamiento,
                COALESCE(ce.pct_hacinamiento_critico, 0) AS c_hacinamiento_crit,
                100.0 - COALESCE(ce.pct_computadora, 0) AS c_sin_computadora
            FROM radios_misiones r
            LEFT JOIN censo2022_variables ce ON r.redcode = ce.redcode
        """,
        "components": [
            ("c_nbi", "c_nbi", False),
            ("c_sin_cloacas", "c_sin_cloacas", False),
            ("c_piso", "c_piso", False),
            ("c_hacinamiento", "c_hacinamiento", False),
            ("c_hacinamiento_crit", "c_hacinamiento_crit", False),
            ("c_sin_computadora", "c_sin_computadora", False),
        ],
    },
    {
        "id": "health_access",
        "sql": """
            SELECT r.redcode,
                COALESCE(o.accessibility_healthcare_min, 300) AS c_healthcare_time,
                COALESCE(ce.pct_cobertura_salud, 0) AS c_health_coverage,
                COALESCE(ce.pct_nbi, 0) AS c_nbi,
                COALESCE(ce.pct_adultos_mayores, 0) AS c_elderly,
                COALESCE(ce.pct_menores_18, 0) AS c_children,
                COALESCE(ce.densidad_hab_km2, 0) AS c_pop_density
            FROM radios_misiones r
            LEFT JOIN oxford_accessibility o ON r.redcode = o.redcode
            LEFT JOIN censo2022_variables ce ON r.redcode = ce.redcode
        """,
        "components": [
            ("c_healthcare_time", "c_healthcare_time", False),   # more time = worse access
            ("c_health_coverage", "c_health_coverage", True),    # more coverage = less gap
            ("c_nbi", "c_nbi", False),                           # more NBI = more vulnerability
            ("c_elderly", "c_elderly", False),                    # more elderly = more demand
            ("c_children", "c_children", False),                  # more children = more demand
            ("c_pop_density", "c_pop_density", False),            # more density = more demand
        ],
    },
    {
        "id": "education_capital",
        "sql": """
            SELECT r.redcode,
                COALESCE(ce.pct_sin_instruccion, 0) AS c_no_schooling,
                COALESCE(ce.pct_secundario_comp, 0) AS c_secondary_plus,
                COALESCE(ce.pct_terciario, 0) + COALESCE(ce.pct_universitario, 0) AS c_higher_edu,
                COALESCE(ce.pct_universitario, 0) AS c_university
            FROM radios_misiones r
            LEFT JOIN censo2022_variables ce ON r.redcode = ce.redcode
        """,
        "components": [
            ("c_no_schooling", "c_no_schooling", False),      # more = less capital
            ("c_secondary_plus", "c_secondary_plus", True),   # more secondary+ = more capital (invert)
            ("c_higher_edu", "c_higher_edu", True),           # more higher edu = more capital (invert)
            ("c_university", "c_university", True),           # more university = more capital (invert)
        ],
    },
    {
        "id": "education_flow",
        "sql": """
            SELECT r.redcode,
                COALESCE(ce.tasa_inasistencia_6a12, 0) AS c_dropout_primary,
                COALESCE(ce.tasa_inasistencia_13a18, 0) AS c_dropout_secondary,
                COALESCE(ce.tasa_maternidad_adolescente, 0) AS c_teen_pregnancy
            FROM radios_misiones r
            LEFT JOIN censo2022_variables ce ON r.redcode = ce.redcode
        """,
        "components": [
            ("c_dropout_primary", "c_dropout_primary", False),       # more = worse flow
            ("c_dropout_secondary", "c_dropout_secondary", False),   # more = worse flow
            ("c_teen_pregnancy", "c_teen_pregnancy", False),          # more = worse flow
        ],
    },
]


def create_duckdb_conn(
    tables: list[str] | None = None,
    overrides: dict[str, str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with all radio tables loaded from parquets.

    overrides: optional {table_name: absolute_path} to use instead of RADIO_DATA_DIR.
    """
    if tables is None:
        tables = RADIO_TABLES
    overrides = overrides or {}
    conn = duckdb.connect()
    loaded = 0
    for table_name in tables:
        path = overrides.get(table_name) or os.path.join(RADIO_DATA_DIR, f"{table_name}.parquet")
        if os.path.exists(path):
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
            loaded += 1
        else:
            # Create empty table so queries don't fail
            conn.execute(f"CREATE TABLE {table_name} (redcode VARCHAR)")
    print(f"  Loaded {loaded}/{len(tables)} radio tables from parquets")
    return conn


def fetch_radio_data(conn, sql: str) -> pd.DataFrame:
    """Execute SQL query on DuckDB and return DataFrame."""
    return conn.execute(sql).fetchdf()


def join_to_h3(radio_df: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Project radio-level data onto H3 hexagons via the dasymetric crosswalk.

    Building-weighted average where buildings exist (~69K hex in Misiones).
    Hexagons without buildings get no value: census data only where people live.
    """
    value_cols = [c for c in radio_df.columns if c != "redcode"]

    def weighted_avg(g):
        w = g["weight"].values
        w_sum = w.sum()
        if w_sum == 0:
            return pd.Series({c: np.nan for c in value_cols})
        return pd.Series({c: np.average(g[c].values, weights=w) for c in value_cols})

    merged_dasy = crosswalk.merge(radio_df, on="redcode", how="inner")
    return merged_dasy.groupby("h3index", group_keys=False).apply(
        weighted_avg, include_groups=False
    ).reset_index()


def normalize_percentile(series: pd.Series) -> pd.Series:
    """Compute percentile rank 0-100 for a series."""
    valid = series.notna()
    result = pd.Series(np.nan, index=series.index)
    if valid.sum() > 1:
        result[valid] = series[valid].rank(pct=True) * 100.0
    return result


def compute_analysis(
    conn,
    crosswalk: pd.DataFrame,
    analysis_def: dict,
    emit_diagnostics: bool = False,
    output_dir: str = OUTPUT_DIR,
    mode: str = 'local',
    goalposts: dict | None = None,
) -> pd.DataFrame:
    """Compute a single composite analysis using PCA-validated geometric mean.

    mode='local': percentile rank within this territory's hex set (default).
    mode='comparable': fixed goalpost normalization — cross-territory comparable.
    """
    analysis_id = analysis_def["id"]
    components = analysis_def["components"]

    # Fetch radio-level data
    radio_data = fetch_radio_data(conn, analysis_def["sql"])
    print(f"    Radio data: {len(radio_data)} rows, "
          f"cols: {[c for c in radio_data.columns if c != 'redcode']}")

    # Project to H3 (dasymetric only: census data exists where people live)
    hex_data = join_to_h3(radio_data, crosswalk)
    print(f"    H3 hexagons: {len(hex_data):,}")

    # Normalize each component
    gp_indicators = goalposts.get('indicators', {}) if goalposts else {}
    for out_col, sql_col, invert in components:
        raw = hex_data[sql_col].astype(float)
        # Preserve the dasymetric (building-weighted) physical value before it is
        # overwritten by the percentile/goalpost score. For census layers this is
        # the estimated % per hexagon (e.g. c_nbi_raw = estimated NBI %).
        hex_data[out_col + "_raw"] = raw.round(1)
        if mode == 'comparable' and out_col in gp_indicators:
            gp = gp_indicators[out_col]
            hex_data[out_col] = score_with_goalposts(
                raw, gp['lo'], gp['hi'], invert=gp.get('invert', invert)
            )
        else:
            if invert:
                raw = -raw
            hex_data[out_col] = normalize_percentile(raw)

    comp_cols = [c[0] for c in components]

    # ── PCA diagnostics (always run for audit) ───────────────────────────
    diagnostics = run_full_diagnostics(hex_data, comp_cols, corr_threshold=0.70)

    kmo = diagnostics["kmo_bartlett"].get("kmo_overall")
    if kmo is not None:
        print(f"    KMO: {kmo:.3f} {'OK' if kmo >= 0.60 else 'WARNING < 0.60'}")

    # Variable selection: locked from goalposts in comparable mode
    if mode == 'comparable' and goalposts:
        locked = goalposts.get('pca_variable_selection', {}).get(analysis_id)
        if locked:
            retained = [c for c in locked if c in comp_cols]
            dropped  = [c for c in comp_cols if c not in retained]
            print(f"    [comparable] Locked selection ({len(retained)}): {retained}")
            if dropped:
                print(f"    [comparable] Excluded: {dropped}")
        else:
            retained = diagnostics["variable_selection"]["retained"]
            dropped  = diagnostics["variable_selection"]["dropped"]
            print(f"    [comparable] No lock found, using PCA selection: {retained}")
    else:
        retained = diagnostics["variable_selection"]["retained"]
        dropped  = diagnostics["variable_selection"]["dropped"]
        if dropped:
            print(f"    Dropped (|r|>0.70): {dropped}")
        print(f"    Retained ({len(retained)}): {retained}")

    # ── Geometric mean score (HDI-style, floor=1.0) ─────────────────────
    hex_data["score"] = geometric_mean_score(hex_data, retained, floor=1.0).round(1)

    # ── Diagnostics report ───────────────────────────────────────────────
    if emit_diagnostics:
        generate_report(analysis_id, diagnostics, output_dir=output_dir)

    # Round component columns
    for out_col, _, _ in components:
        hex_data[out_col] = hex_data[out_col].round(1)

    # Per-hexagon magnitude from the dasymetric crosswalk (mass-preserving):
    # n_edificios = building count, hogares_estimados = Σ per-building est_hogares.
    # Households are the natural denominator for the census deprivation indicators
    # (all are % of households). Summed across all hexes these match radio totals.
    mag_cols: list[str] = []
    if {"n_buildings", "est_hogares"}.issubset(crosswalk.columns):
        mag = (
            crosswalk.groupby("h3index")
            .agg(n_edificios=("n_buildings", "sum"),
                 hogares_estimados=("est_hogares", "sum"))
            .reset_index()
        )
        mag["n_edificios"] = mag["n_edificios"].astype("int64")
        mag["hogares_estimados"] = mag["hogares_estimados"].round(0).astype("int64")
        hex_data = hex_data.merge(mag, on="h3index", how="left")
        mag_cols = ["n_edificios", "hogares_estimados"]

    # Select output columns (percentile components + preserved _raw + magnitude)
    raw_cols = [f"{c}_raw" for c in comp_cols if f"{c}_raw" in hex_data.columns]
    out_cols = ["h3index", "score"] + comp_cols + raw_cols + mag_cols
    result = hex_data[out_cols].copy()

    # Stats
    non_null = result["score"].notna().sum()
    avg = result["score"].mean()
    median = result["score"].median()
    print(f"    Score (geometric mean): n={non_null:,}, avg={avg:.1f}, median={median:.1f}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Compute satellite composite scores at H3 res-9 (PCA + geometric mean)"
    )
    parser.add_argument(
        "--territory", default="misiones",
        help="Territory ID (default: misiones). Controls radio table and crosswalk paths."
    )
    parser.add_argument(
        "--only", default=None,
        help="Comma-separated list of analysis IDs to compute (default: all)"
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory (default: OUTPUT_DIR or OUTPUT_DIR/<territory>)"
    )
    parser.add_argument(
        "--diagnostics", action="store_true",
        help="Emit PCA/KMO/correlation diagnostic JSON per analysis"
    )
    parser.add_argument(
        "--mode", choices=["comparable", "local"], default="local",
        help="comparable: fixed goalpost normalization (cross-territory). local: percentile rank within territory (default)."
    )
    args = parser.parse_args()

    # ── Territory-aware paths ─────────────────────────────────────────────
    territory_id = args.territory
    RADIO_TABLE = f"radios_{territory_id}"

    if territory_id == 'misiones':
        crosswalk_path = CROSSWALK_PATH
        output_dir = args.output_dir or OUTPUT_DIR
        radio_data_overrides = {}
    else:
        t_dir = os.path.join(OUTPUT_DIR, territory_id)
        # Crosswalk naming convention: h3_radio_crosswalk_{territory_id}.parquet
        crosswalk_path = os.path.join(t_dir, f"h3_radio_crosswalk_{territory_id}.parquet")
        output_dir = args.output_dir or t_dir
        # Override paths for files that live in t_dir, not radio_data/
        radio_data_overrides = {
            RADIO_TABLE: os.path.join(t_dir, f"radios_{territory_id}.parquet"),
            f"censo2022_variables_{territory_id}": os.path.join(
                t_dir, f"censo2022_variables_{territory_id}.parquet"
            ),
            "oxford_accessibility": os.path.join(
                t_dir, f"oxford_accessibility_{territory_id}.parquet"
            ),
        }

    # Patch SQL strings and radio table list for this territory
    censo_table = "censo2022_variables" if territory_id == 'misiones' else f"censo2022_variables_{territory_id}"
    for a_def in ANALYSIS_DEFS:
        a_def["sql"] = a_def["sql"].replace("radios_misiones", RADIO_TABLE)
        if territory_id != 'misiones':
            a_def["sql"] = a_def["sql"].replace("censo2022_variables", censo_table)
    radio_tables = [
        RADIO_TABLE if t == "radios_misiones" else
        censo_table if t == "censo2022_variables" and territory_id != 'misiones' else t
        for t in RADIO_TABLES
    ]

    goalposts = load_goalposts() if args.mode == "comparable" else None
    if goalposts:
        print(f"  Loaded goalposts v{goalposts.get('version', '?')} "
              f"({goalposts.get('computed', '?')})")

    # agri_potential runs in process_raster_to_h3.py (hex-level zonal stats over a
    # GEE raster). Point there instead of failing with "no matching analyses".
    RASTER_BASED = {"agri_potential"}

    # Filter analyses if --only specified
    analyses = list(ANALYSIS_DEFS)
    if args.only:
        only_ids = set(args.only.split(","))
        overlap = only_ids & RASTER_BASED
        if overlap:
            print(f"ERROR: {sorted(overlap)} are raster-based. Run via:")
            print(f"  python pipeline/process_raster_to_h3.py --analysis {','.join(sorted(overlap))}")
            return 1
        analyses = [a for a in analyses if a["id"] in only_ids]
        if not analyses:
            print(f"ERROR: No matching analyses for --only={args.only}")
            return 1

    print("=" * 60)
    print("  Compute Satellite Composite Scores")
    print(f"  Territory: {territory_id}  (table: {RADIO_TABLE})")
    print(f"  Analyses: {len(analyses)}")
    print("=" * 60)

    # Load crosswalks
    print("\nLoading crosswalks...")
    crosswalk = pd.read_parquet(crosswalk_path)
    print(f"  Dasymetric: {len(crosswalk):,} rows, {crosswalk['h3index'].nunique():,} unique hexagons")

    # Load radio data tables into DuckDB
    print("Loading radio tables into DuckDB...")
    conn = create_duckdb_conn(tables=radio_tables, overrides=radio_data_overrides)

    t0 = time.time()
    results = {}

    for i, analysis_def in enumerate(analyses, 1):
        aid = analysis_def["id"]
        print(f"\n[{i}/{len(analyses)}] Computing {aid}...")

        try:
            # Dasymetric only: projecting census radios onto hexagons without
            # buildings creates artifacts (rivers, forests).
            result = compute_analysis(
                conn, crosswalk, analysis_def,
                emit_diagnostics=args.diagnostics,
                output_dir=output_dir,
                mode=args.mode,
                goalposts=goalposts,
            )
            out_path = os.path.join(output_dir, f"sat_{aid}.parquet")
            write_h3_parquet(result, out_path)
            size_kb = os.path.getsize(out_path) / 1024
            print(f"    Output: {out_path} ({size_kb:.0f} KB)")
            results[aid] = len(result)
        except Exception as e:
            print(f"    ERROR: {e}")
            import traceback
            traceback.print_exc()
            results[aid] = 0

    conn.close()
    elapsed = time.time() - t0

    print(f"\n{'=' * 60}")
    print(f"  Completed in {elapsed:.0f}s")
    for aid, count in results.items():
        status = f"{count:,} hexes" if count > 0 else "FAILED"
        print(f"    {aid}: {status}")
    print(f"{'=' * 60}")

    failed = sum(1 for c in results.values() if c == 0)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())