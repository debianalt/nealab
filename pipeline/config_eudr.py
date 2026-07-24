"""
Shared configuration for the EUDR deforestation compliance pipeline.

Covers Chaco, Salta, Santiago del Estero, and Formosa provinces —
Argentina's deforestation frontier for soy, cattle, and wood exports.

Import from this module instead of defining local constants.
"""

import os

# ── Spatial ───────────────────────────────────────────────────────────────
# Individual province bounding boxes [W, S, E, N] — padded ~0.1 deg
EUDR_PROVINCES = {
    # NOA
    "salta":              [-68.70, -26.50, -62.30, -21.90],
    "jujuy":              [-67.10, -24.40, -64.10, -21.70],
    "tucuman":            [-66.60, -28.00, -64.50, -26.00],
    "catamarca":          [-69.00, -30.20, -64.80, -25.80],
    "santiago_del_estero": [-65.30, -30.60, -61.60, -25.90],
    # NEA
    "formosa":            [-62.20, -27.00, -57.40, -22.90],
    "chaco":              [-63.50, -28.00, -58.40, -24.80],
    "corrientes":         [-59.80, -30.80, -55.60, -27.20],
    "misiones":           [-56.10, -28.20, -53.55, -25.44],
    "entre_rios":         [-60.80, -34.10, -57.80, -30.10],
}

# Combined bounding box covering all 10 NOA+NEA provinces (with padding)
EUDR_BBOX = [-69.00, -34.10, -53.55, -21.70]

H3_EUDR_RESOLUTION = 7  # ~5.16 km2 per hex, ~112K hexagons for 4 provinces

# ── EUDR Regulation ──────────────────────────────────────────────────────
EUDR_CUTOFF_YEAR = 2020  # Deforestation after 31 Dec 2020 is non-compliant
EUDR_COMMODITIES = ["soya", "cattle", "wood"]

# ── Hansen GFC vintage ───────────────────────────────────────────────────
# Bump these three on each annual Hansen release. Frontend texts that cite the
# version live in src/routes/eudr/check/+page.svelte and
# src/lib/content/methodology.ts; the served vintage comes from eudr_meta.json
# written by combine_eudr_parquets.py.
HANSEN_ASSET = "UMD/hansen/global_forest_change_2025_v1_13"
HANSEN_VERSION = "v1.13"
HANSEN_MAX_YEAR = 2025   # last loss year in the asset (lossyear code 25)

# ── MapBiomas Argentina ──────────────────────────────────────────────────
# Collection 2 (1985-2024, 18 classes) is the current one; Collection 1 covered
# 1998-2022 and is superseded. Collections lag one year, so there is no 2025.
# Public assets: no Earth Engine permissions needed beyond a registered account.
MAPBIOMAS_C2_ASSET = ("projects/mapbiomas-public/assets/argentina/lulc/"
                      "collection2/mapbiomas_argentina_collection2_"
                      "integration_v3")
# Vegetation-loss / secondary-vegetation module built on the same collection.
MAPBIOMAS_C2_DEFOR_ASSET = ("projects/mapbiomas-public/assets/argentina/lulc/"
                            "collection2/mapbiomas_argentina_collection2_"
                            "deforestation_secondary_vegetation_v1")
MAPBIOMAS_C2_FIRST_YEAR = 1985
MAPBIOMAS_C2_LAST_YEAR = 2024

# Land-cover class codes, per the official Collection 2 legend for ARGENTINA:
# https://argentina.mapbiomas.org/wp-content/uploads/sites/12/2025/09/Leyenda_Code.pdf
#
# NOTE: do NOT use the Brazilian names here ("formacion forestal", "formacion
# sabanica"). Those belong to MapBiomas Brasil. In the Argentine legend classes
# 3, 4 and 6 are all sub-classes of category "1. Bosques":
#   3 = Bosques cerrados  (closed forests)
#   4 = Bosques abiertos  (open forests)
#   6 = Bosques inundables (flooded forests)
# and class 5 (mangrove) does not exist in the Argentine legend at all.
MB_CLASS_FORESTRY = 9            # 3.3 Silvicultura (forest plantation)
MB_CLASS_CLOSED_FOREST = 3       # 1.1 Bosques cerrados (incl. dry Chaco, Paranaense)
MB_CLASSES_OPEN_FOREST = (4, 6)  # 1.2 Bosques abiertos + 1.3 Bosques inundables
MB_CLASSES_NATIVE = (3, 4, 6)    # native woody vegetation = the whole "Bosques" category

# Backwards-compatible aliases. Kept so existing imports do not break, but the
# "SAVANNA"/"FOREST" spellings are misnomers under the Argentine legend.
MB_CLASS_FOREST = MB_CLASS_CLOSED_FOREST
MB_CLASSES_SAVANNA = MB_CLASSES_OPEN_FOREST

# ── GEE Export ───────────────────────────────────────────────────────────
EXPORT_SCALE = 100       # metres per pixel (Hansen native 30m, 100m for efficiency)
DRIVE_FOLDER = "spatia-eudr"

# ── Cloudflare R2 ────────────────────────────────────────────────────────
R2_BUCKET = "neahub"
R2_EUDR_PREFIX = "data/eudr"

# ── Validation thresholds ────────────────────────────────────────────────
MIN_EUDR_HEXAGONS = 150_000  # 10 NOA+NEA provinces at res-7
MAX_NULL_FRACTION = 0.20     # max 20% nulls acceptable
SCORE_RANGE = (0, 100)

# ── Paths ────────────────────────────────────────────────────────────────
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PIPELINE_DIR, "output", "eudr")
GRID_PATH = os.path.join(OUTPUT_DIR, "hexagons_eudr.geojson")
GRID_LITE_PATH = os.path.join(OUTPUT_DIR, "hexagons_eudr_lite.geojson")
PARQUET_PATH = os.path.join(OUTPUT_DIR, "eudr_deforestation.parquet")

# Province boundary GeoJSON (from IGN)
PROJECT_ROOT = os.path.dirname(PIPELINE_DIR)
BOUNDARY_PATH = os.path.join(
    PROJECT_ROOT, "src", "lib", "data", "eudr_provinces_boundary.json"
)

# ── Risk Score Weights ───────────────────────────────────────────────────
# risk_score = w_loss * loss_post_2020 + w_fire * fire_post_2020
#            + w_noforest * (1 - forest_cover_2020)
WEIGHT_LOSS_POST_2020 = 0.70
WEIGHT_FIRE_POST_2020 = 0.20
WEIGHT_NO_FOREST_2020 = 0.10
