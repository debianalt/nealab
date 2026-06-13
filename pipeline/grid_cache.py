"""
Cache the static H3 grid + admin crosswalk of a territory in Cloudflare R2.

The hex grid (`hexagons.geojson`) and admin crosswalk (`h3_admin_crosswalk.parquet`)
are static — they only change if administrative boundaries change. Regenerating them
on every CI run requires re-downloading an admin shapefile from an external server
(INE Paraguay, GADM/UC-Davis), which is slow and flaky (the cause of the recurring
"curl exit 28" failures). Instead we persist them once in R2 and pull from there.

Artifacts are stored gzip-compressed (`.gz` suffix) because the hex GeoJSON can exceed
wrangler's 300 MiB upload cap (Corrientes is ~412 MB raw, ~60 MB gzipped) and the H3
polygon GeoJSON compresses ~7x. Pull transparently decompresses to the canonical path.

R2 key convention (mirrors the baseline cache `data/{prefix}temporal_baseline`):
  data/grid/{output_prefix}hexagons.geojson.gz
  data/grid/{output_prefix}h3_admin_crosswalk.parquet.gz

Usage:
  python pipeline/grid_cache.py --pull --territory alto_parana_py   # CI: fetch from R2
  python pipeline/grid_cache.py --push --territory alto_parana_py   # seed/refresh R2

--pull exits 0 only if BOTH artifacts were obtained, so callers can fall back to
regenerating from source on a cache miss. --push uploads both local artifacts (needs
CLOUDFLARE_API_TOKEN, like every wrangler upload).
"""

import argparse
import gzip
import os
import shutil
import sys

from config import OUTPUT_DIR, get_territory
from upload_to_r2 import download_file, upload_file

GRID_ARTIFACTS = ["hexagons.geojson", "h3_admin_crosswalk.parquet"]


def _paths(territory_id: str):
    """Return [(local_path, r2_key), ...] for the territory's grid artifacts.

    r2_key is the gzipped object key (`.gz` suffix); local_path is the
    decompressed path the pipeline reads.
    """
    prefix = get_territory(territory_id)["output_prefix"]  # e.g. "alto_parana_py/" or ""
    out_dir = os.path.join(OUTPUT_DIR, prefix.rstrip("/")) if prefix else OUTPUT_DIR
    pairs = []
    for name in GRID_ARTIFACTS:
        local_path = os.path.join(out_dir, name)
        r2_key = f"data/grid/{prefix}{name}.gz"
        pairs.append((local_path, r2_key))
    return pairs


def _gzip(src: str, dst: str):
    with open(src, "rb") as f_in, gzip.open(dst, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)


def _gunzip(src: str, dst: str):
    with gzip.open(src, "rb") as f_in, open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)


def pull(territory_id: str) -> bool:
    """Download + decompress both grid artifacts from R2. True only if both present."""
    ok = True
    for local_path, r2_key in _paths(territory_id):
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            print(f"  [skip] already local: {local_path}")
            continue
        tmp_gz = local_path + ".gz"
        if not download_file(r2_key, tmp_gz):
            ok = False
            continue
        _gunzip(tmp_gz, local_path)
        os.remove(tmp_gz)
        print(f"  [ok] decompressed -> {local_path} ({os.path.getsize(local_path) / 1e6:.1f} MB)")
    return ok


def push(territory_id: str) -> bool:
    """Compress + upload both grid artifacts to R2 (immutable, no archive versioning)."""
    ok = True
    for local_path, r2_key in _paths(territory_id):
        if not os.path.exists(local_path):
            print(f"  [x] missing local artifact, cannot push: {local_path}")
            ok = False
            continue
        tmp_gz = local_path + ".gz"
        _gzip(local_path, tmp_gz)
        size_mb = os.path.getsize(tmp_gz) / (1024 * 1024)
        if size_mb > 300:
            print(f"  [x] {r2_key} is {size_mb:.0f} MiB gzipped, exceeds wrangler 300 MiB cap")
            ok = False
        elif not upload_file(tmp_gz, r2_key, versioned=False):
            ok = False
        os.remove(tmp_gz)
    return ok


def main():
    parser = argparse.ArgumentParser(description="Pull/push a territory's H3 grid to R2")
    parser.add_argument("--territory", required=True, help="Territory ID from config.py")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--pull", action="store_true", help="Download grid from R2 cache")
    mode.add_argument("--push", action="store_true", help="Upload grid to R2 cache")
    args = parser.parse_args()

    success = pull(args.territory) if args.pull else push(args.territory)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
