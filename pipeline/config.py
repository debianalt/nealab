"""
Shared configuration for the Spatia satellite pipeline.

All constants used across pipeline scripts are centralised here.
Import from this module instead of defining local constants.
"""

import os
from datetime import date, timedelta

# ── Spatial ───────────────────────────────────────────────────────────────
MISIONES_BBOX = [-56.10, -28.20, -53.55, -25.44]  # [W, S, E, N] — padded to cover edge hexagons
POSADAS_BBOX  = [-56.05, -27.55, -55.65, -27.15]  # Departamento Capital, Misiones
H3_RESOLUTION = 9

# ── Multi-territory configuration ────────────────────────────────────────
# Each territory can run the full satellite pipeline independently.
# 'output_prefix' maps to R2 path prefix ('' = root = Misiones legacy paths).
TERRITORY_CONFIGS: dict[str, dict] = {
    'misiones': {
        'id': 'misiones',
        'label': 'Misiones',
        'country': 'ar',
        'bbox': [-56.10, -28.20, -53.55, -25.44],   # padded
        'admin_level': 'departamento',
        'admin_col': 'dpto',                          # column name in crosswalk
        'admin_collection': None,                     # uses AR radio crosswalk
        'admin_filter': None,
        'output_prefix': '',                          # R2: data/sat_*.parquet
        'export_scale': 100,
    },
    'itapua_py': {
        'id': 'itapua_py',
        'label': 'Itapúa',
        'country': 'py',
        'bbox': [-57.40, -27.70, -54.60, -26.10],   # padded to cover edge hexagons
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',  # provisional; see explore_itapua_admin.py
        'admin_filter': ('ADM1_NAME', 'Itapua'),
        'output_prefix': 'itapua_py/',               # R2: data/itapua_py/sat_*.parquet
        'export_scale': 100,
    },
    'corrientes': {
        'id': 'corrientes',
        'label': 'Corrientes',
        'country': 'ar',
        'bbox': [-59.85, -30.90, -55.45, -27.10],  # fixed: was [-59.50,-30.00,-56.00,-27.00], missed eastern strip (Santo Tome) and south
        'admin_level': 'departamento',
        'admin_col': 'dpto',
        'admin_collection': None,                    # uses local ARG_adm2.shp (same as Misiones)
        'admin_filter': ('NAME_1', 'Corrientes'),   # GADM: filter province by NAME_1
        'output_prefix': 'corrientes/',              # R2: data/corrientes/sat_*.parquet
        'export_scale': 100,
    },
    'alto_parana_py': {
        'id': 'alto_parana_py',
        'label': 'Alto Paraná',
        'country': 'py',
        # Derived from INE district dissolve (build_territory_boundary_mask.py),
        # padded 0.1deg. Raw bounds: [-55.5506, -26.2571, -54.3458, -24.4762].
        'bbox': [-55.65, -26.36, -54.25, -24.38],   # [W, S, E, N]
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        # Admin source = INE 2022 census cartography (22 distritos), NOT GAUL/GADM:
        # both GAUL 2015 and GADM 4.1 only have 18 (missing Iruña, Santa Fe del
        # Paraná, Dr. Raúl Peña, Tavapy — created post-2015). Built via
        # ine_cartografia_to_geojson.py -> output/alto_parana_py_ine_distritos.geojson,
        # consumed by build_admin_crosswalk.py --source geojson.
        'admin_collection': None,
        'admin_filter': ('ADM1_NAME', 'Alto Parana'),  # ascii-folded match vs INE 'ALTO PARANÁ'
        'output_prefix': 'alto_parana_py/',          # R2: data/alto_parana_py/sat_*.parquet
        'export_scale': 100,
    },
    # ── NEA argentino — pending v1.1 re-baseline (Gran Chaco biome) ────
    # These territories are OUTSIDE the Paraná Atlantic-Forest reference universe
    # frozen in goalposts.json v1. Adding them triggers a deliberate methodological
    # revision (Opción B, decided 2026-05-23): re-baseline Tier 2 P2/P98 over pool
    # of Mis+Cor+Ita+AP+Chaco+Formosa (+ ideally Paraná-BR Mata Atlântica). Until
    # the bump executes, scoring these territories with v1 bounds will saturate
    # (c_precipitation lo=1444 vs Chaco typical <1300; c_ndvi will clip too).
    'chaco': {
        'id': 'chaco',
        'label': 'Chaco',
        'country': 'ar',
        # padded ~0.1deg, matches config_eudr.py:EUDR_PROVINCES['chaco'].
        'bbox': [-63.50, -28.00, -58.40, -24.80],
        'admin_level': 'departamento',
        'admin_col': 'dpto',
        'admin_collection': None,                    # uses local ARG_adm2.shp
        'admin_filter': ('NAME_1', 'Chaco'),         # GADM: filter province by NAME_1
        'output_prefix': 'chaco/',                   # R2: data/chaco/sat_*.parquet
        'export_scale': 100,
        'codprov_indec': 22,                         # INDEC province code for radios censales 2022
        'biome': 'gran_chaco',                       # NOT paraná_atlantic_forest — triggers v1.1 bump
    },
    'formosa': {
        'id': 'formosa',
        'label': 'Formosa',
        'country': 'ar',
        # padded ~0.1deg, matches config_eudr.py:EUDR_PROVINCES['formosa'].
        'bbox': [-62.20, -27.00, -57.40, -22.90],
        'admin_level': 'departamento',
        'admin_col': 'dpto',
        'admin_collection': None,                    # uses local ARG_adm2.shp
        'admin_filter': ('NAME_1', 'Formosa'),       # GADM: filter province by NAME_1
        'output_prefix': 'formosa/',                 # R2: data/formosa/sat_*.parquet
        'export_scale': 100,
        'codprov_indec': 34,
        'biome': 'gran_chaco',                       # NOT paraná_atlantic_forest — triggers v1.1 bump
    },
    # ── Brasil sur — Mata Atlântica interior (compatible biome) ────────
    # Added to v1.1 pool together with Chaco/Formosa. Biome compatible with
    # Mis+Ita+AP (Paraná Atlantic Forest), so in principle they could enter
    # against frozen v1 without bump — but since v1.1 is already happening
    # (Chaco/Formosa = different biome), pooling BR-sur now produces more
    # robust Tier 2 P2/P98 and prevents a future puntual bump if any clipping
    # appears (e.g. c_frost higher in RS).
    # Admin units = municípios (GADM level 2). Brasil tiene ~399 (PR), 295 (SC),
    # 497 (RS) municípios — many, but functional. PostGIS census/IBGE setor
    # censitário load deferred to follow-up; satellite layers only require
    # H3 grid + bbox.
    'parana_br': {
        'id': 'parana_br',
        'label': 'Paraná',
        'country': 'br',
        'bbox': [-54.72, -26.82, -47.92, -22.41],   # GADM admin1 + 0.1deg pad
        'admin_level': 'municipio',
        'admin_col': 'municipio',
        'admin_collection': None,                    # uses local gadm41_BRA_2.shp
        'admin_filter': ('NAME_1', 'Paraná'),
        'output_prefix': 'parana_br/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',           # same as Mis/Ita/AP — compatible
    },
    'santa_catarina_br': {
        'id': 'santa_catarina_br',
        'label': 'Santa Catarina',
        'country': 'br',
        'bbox': [-53.94, -29.46, -48.26, -25.85],
        'admin_level': 'municipio',
        'admin_col': 'municipio',
        'admin_collection': None,
        'admin_filter': ('NAME_1', 'Santa Catarina'),
        'output_prefix': 'santa_catarina_br/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'rio_grande_sul_br': {
        'id': 'rio_grande_sul_br',
        'label': 'Rio Grande do Sul',
        'country': 'br',
        'bbox': [-57.75, -33.85, -49.59, -26.98],
        'admin_level': 'municipio',
        'admin_col': 'municipio',
        'admin_collection': None,
        'admin_filter': ('NAME_1', 'Rio Grande do Sul'),
        'output_prefix': 'rio_grande_sul_br/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',           # RS extends to Pampa in south — minor edge but accepted
    },
    # ── Paraguay — todos los departamentos (Paraná Atlantic Forest oriental) ──
    # Admin source: FAO/GAUL/2015/level2 filtered to ADM0_NAME='Paraguay' + ADM1_NAME.
    # GeoJSONs exported via pipeline/export_py_all.py before running crosswalk.
    # Comparable pool: joins existing PY+AR+BR Paraná Atlantic Forest universe.
    'concepcion_py': {
        'id': 'concepcion_py',
        'label': 'Concepción',
        'country': 'py',
        'bbox': [-62.3, -23.8, -55.4, -20.3],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Concepcion'),
        'output_prefix': 'concepcion_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'san_pedro_py': {
        'id': 'san_pedro_py',
        'label': 'San Pedro',
        'country': 'py',
        'bbox': [-59.4, -25.5, -55.3, -22.3],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'San Pedro'),
        'output_prefix': 'san_pedro_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'cordillera_py': {
        'id': 'cordillera_py',
        'label': 'Cordillera',
        'country': 'py',
        'bbox': [-57.8, -25.4, -56.6, -24.4],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Cordillera'),
        'output_prefix': 'cordillera_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'guaira_py': {
        'id': 'guaira_py',
        'label': 'Guairá',
        'country': 'py',
        'bbox': [-56.7, -26.1, -55.4, -25.1],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Guaira'),
        'output_prefix': 'guaira_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'caaguazu_py': {
        'id': 'caaguazu_py',
        'label': 'Caaguazú',
        'country': 'py',
        'bbox': [-56.7, -25.9, -54.2, -23.3],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Caaguazu'),
        'output_prefix': 'caaguazu_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'caazapa_py': {
        'id': 'caazapa_py',
        'label': 'Caazapá',
        'country': 'py',
        'bbox': [-56.7, -26.9, -55.0, -25.5],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Caazapa'),
        'output_prefix': 'caazapa_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'misiones_py': {
        'id': 'misiones_py',
        'label': 'Misiones',
        'country': 'py',
        'bbox': [-57.5, -27.3, -56.3, -26.7],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        # GAUL has both AR-Misiones and PY-Misiones; export script filters by ADM0_NAME='Paraguay'
        'admin_filter': ('ADM1_NAME', 'Misiones'),
        'output_prefix': 'misiones_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'paraguari_py': {
        'id': 'paraguari_py',
        'label': 'Paraguarí',
        'country': 'py',
        'bbox': [-58.1, -26.7, -56.5, -25.1],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Paraguari'),
        'output_prefix': 'paraguari_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'central_py': {
        'id': 'central_py',
        'label': 'Central',
        'country': 'py',
        'bbox': [-58.2, -25.8, -57.1, -24.9],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Central'),
        'output_prefix': 'central_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'neembucu_py': {
        'id': 'neembucu_py',
        'label': 'Ñeembucú',
        'country': 'py',
        'bbox': [-58.6, -27.6, -57.1, -26.3],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Neembucu'),
        'output_prefix': 'neembucu_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'amambay_py': {
        'id': 'amambay_py',
        'label': 'Amambay',
        'country': 'py',
        'bbox': [-56.6, -23.7, -55.0, -21.7],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Amambay'),
        'output_prefix': 'amambay_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    'canindeyu_py': {
        'id': 'canindeyu_py',
        'label': 'Canindeyú',
        'country': 'py',
        'bbox': [-55.7, -25.3, -53.9, -22.6],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Canindeyu'),
        'output_prefix': 'canindeyu_py/',
        'export_scale': 100,
        'biome': 'parana_atlantic_forest',
    },
    # ── Paraguay — región occidental (Gran Chaco) ─────────────────────────
    'presidente_hayes_py': {
        'id': 'presidente_hayes_py',
        'label': 'Presidente Hayes',
        'country': 'py',
        'bbox': [-62.1, -25.6, -57.4, -21.9],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Presidente Hayes'),
        'output_prefix': 'presidente_hayes_py/',
        'export_scale': 100,
        'biome': 'gran_chaco',
    },
    'boqueron_py': {
        'id': 'boqueron_py',
        'label': 'Boquerón',
        'country': 'py',
        'bbox': [-62.6, -23.2, -59.4, -20.0],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Boqueron'),
        'output_prefix': 'boqueron_py/',
        'export_scale': 100,
        'biome': 'gran_chaco',
    },
    'alto_paraguay_py': {
        'id': 'alto_paraguay_py',
        'label': 'Alto Paraguay',
        'country': 'py',
        'bbox': [-61.2, -23.2, -57.4, -18.4],
        'admin_level': 'distrito',
        'admin_col': 'distrito',
        'admin_collection': 'FAO/GAUL/2015/level2',
        'admin_filter': ('ADM1_NAME', 'Alto Paraguay'),
        'output_prefix': 'alto_paraguay_py/',
        'export_scale': 100,
        'biome': 'gran_chaco',
    },
}

def get_territory(territory_id: str) -> dict:
    """Return territory config, raising KeyError with helpful message if not found."""
    if territory_id not in TERRITORY_CONFIGS:
        raise KeyError(
            f"Unknown territory '{territory_id}'. "
            f"Available: {list(TERRITORY_CONFIGS.keys())}"
        )
    return TERRITORY_CONFIGS[territory_id]

# ── Google Cloud Storage ──────────────────────────────────────────────────
GCS_BUCKET = "spatia-satellite"

# ── Cloudflare R2 ─────────────────────────────────────────────────────────
R2_BUCKET = "neahub"

# ── Flood detection ──────────────────────────────────────────────────────
VV_THRESHOLD_DB = -15   # dB threshold for Sentinel-1 water detection
EXPORT_SCALE = 30       # metres per pixel (S1 GRD native ~10m, 30m for efficiency)
EXPORT_PREFIX = "flood"
FLOOD_GCS_PREFIX = "flood/"

# ── Temporal windows for baseline vs current ─────────────────────────────
BASELINE_START = '2019-01-01'
BASELINE_END   = '2021-12-31'   # 3 stable years, pre-recent-change
_today = date.today()
CURRENT_END    = _today.isoformat()
CURRENT_START  = (_today - timedelta(days=180)).replace(day=1).isoformat()  # ~6 months back, 1st of month

# ── Validation thresholds ────────────────────────────────────────────────
MIN_HEXAGONS = 50_000       # Misiones has ~280K; <50K indicates corrupt data
MAX_NULL_FRACTION = 0.20    # max 20% nulls acceptable
SCORE_RANGE = (0, 100)

# ── Catastro ─────────────────────────────────────────────────────────────
MIN_RADIOS_CATASTRO = 1_800

# ── Paths ────────────────────────────────────────────────────────────────
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PIPELINE_DIR, "output")
GRID_PATH = os.path.join(OUTPUT_DIR, "hexagons.geojson")
PARQUET_PATH = os.path.join(OUTPUT_DIR, "hex_flood_risk.parquet")
CATASTRO_PARQUET_PATH = os.path.join(OUTPUT_DIR, "catastro_by_radio.parquet")
CATASTRO_CHANGES_PATH = os.path.join(OUTPUT_DIR, "catastro_changes_summary.parquet")

# ── Overture Maps (walkthru.earth H3-indexed indices) ───────────────────
OVERTURE_RELEASE = "2026-03-18.0"
OVERTURE_THEMES = ["buildings", "transportation", "places", "base"]
OVERTURE_BASE_URL = (
    "https://data.source.coop/walkthru-earth/indices/"
    "{theme}-index/v1/release={release}/h3/h3_res=9/data.parquet"
)
MIN_OVERTURE_HEXAGONS = 10_000  # Sparse layers (transport, places) have <50K populated cells

# ── EMSA (red eléctrica — media y alta tensión) ─────────────────────
EMSA_URL = (
    "http://datos.energia.gob.ar/dataset/"
    "ff99e7be-7bab-4617-9588-9a74ae046a40/resource/"
    "c8c0c8ff-5597-46d0-8b49-bbacd1560f29/download/"
    "-misiones-media-tensin-lneas.zip"
)
EMSA_PARQUET = os.path.join(OUTPUT_DIR, "emsa_powerlines.parquet")
MIN_EMSA_HEXAGONS = 1_000  # Sparse: only hexagons crossed by powerlines
