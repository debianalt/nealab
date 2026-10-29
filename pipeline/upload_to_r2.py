"""
Upload pipeline outputs to Cloudflare R2.

Uses wrangler CLI for R2 uploads. Requires CLOUDFLARE_API_TOKEN in environment.
Overwrites the latest path. Archiving a dated copy to archive/{date}/ is opt-in
(versioned=True / --versioned): the R2 free tier is capped at 10 GB and scheduled
runs were piling up archive copies every month.

Usage:
  python pipeline/upload_to_r2.py --file pipeline/output/hex_flood_risk.parquet --dest data/hex_flood_risk.parquet
  python pipeline/upload_to_r2.py --all  # upload all outputs
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

from config import R2_BUCKET, OUTPUT_DIR


UPLOAD_MAP = {
    "hex_flood_risk.parquet": "data/hex_flood_risk.parquet",
    "h3_radio_crosswalk.parquet": "data/h3_radio_crosswalk.parquet",
    "h3_parent_crosswalk.parquet": "data/h3_parent_crosswalk.parquet",
    "hexagons-v2.pmtiles": "tiles/hexagons-v2.pmtiles",
    "catastro_by_radio.parquet": "data/catastro_by_radio.parquet",
    "catastro_changes_summary.parquet": "data/catastro_changes_summary.parquet",
    "overture_buildings.parquet": "data/overture_buildings.parquet",
    "overture_transportation.parquet": "data/overture_transportation.parquet",
    "overture_places.parquet": "data/overture_places.parquet",
    "overture_base.parquet": "data/overture_base.parquet",
}


R2_PUBLIC_BASE = "https://pub-580c676bec7f4eeb96d7d30559a3cab7.r2.dev"


def download_file(r2_key: str, local_path: str) -> bool:
    """Download a file from R2 public URL. Returns True on success."""
    import urllib.request
    import urllib.error

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    url = f"{R2_PUBLIC_BASE}/{r2_key}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "spatia-pipeline/1.0"})
        with urllib.request.urlopen(req) as resp, open(local_path, "wb") as f:
            f.write(resp.read())
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            size_kb = os.path.getsize(local_path) / 1024
            print(f"  [ok] Downloaded {r2_key} ({size_kb:.0f} KB)")
            return True
    except urllib.error.HTTPError:
        pass
    except Exception as e:
        print(f"  [warn] Download error for {r2_key}: {e}")

    # Clean up empty/failed file
    if os.path.exists(local_path) and os.path.getsize(local_path) == 0:
        os.remove(local_path)
    print(f"  [skip] {r2_key} not found in R2 (first run?)")
    return False


IMMUTABLE = "public, max-age=31536000, immutable"


def _remote_size(r2_key: str) -> int | None:
    """Content-Length of the object as R2 serves it (public bucket URL, cache-busted)."""
    import urllib.request
    url = f"{R2_PUBLIC_BASE}/{r2_key}?verify={int(datetime.now().timestamp())}"
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "spatia-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return int(resp.headers.get("Content-Length", -1))
    except Exception:
        return None


def _run_wrangler_upload(local_path: str, r2_key: str, cache_control: str = IMMUTABLE) -> bool:
    """Execute a single wrangler R2 upload and confirm R2 now serves a file of the
    same size; return True only then.

    shell=True only on Windows (npx is npx.cmd there). On Linux shell=True with a
    list runs just the first item: `npx` alone, which uploads nothing and exits 0 —
    every CI upload from June to October 2026 "succeeded" that way while R2 kept
    the old objects. The size check catches any such silent no-op.
    """
    result = subprocess.run(
        ["npx", "wrangler", "r2", "object", "put",
         f"{R2_BUCKET}/{r2_key}", "--file", local_path, "--remote",
         "--cache-control", cache_control],
        capture_output=True,
        shell=(os.name == "nt"),
        encoding="utf-8",
        errors="replace",
    )

    if result.returncode != 0:
        err = result.stderr.strip().encode("ascii", "replace").decode("ascii")
        print(f"  [x] wrangler failed for {r2_key}: {err}")
        return False
    local_size = os.path.getsize(local_path)
    remote = _remote_size(r2_key)
    if remote != local_size:
        print(f"  [x] {r2_key}: R2 serves {remote} bytes, local file is {local_size} — upload did not land")
        return False
    return True


def upload_file(local_path: str, r2_key: str, versioned: bool = False, cache_control: str = IMMUTABLE) -> bool:
    """
    Upload a single file to R2 using wrangler.

    If versioned=True, uploads to data/archive/{YYYY-MM-DD}/{filename} first,
    then to the latest path (r2_key).
    Retries up to 2 attempts with a brief pause between. Objects refreshed in place
    under a fixed URL (the flood current extent) need a short cache_control: the
    default immutable year would pin browsers to the first copy.
    """
    if not os.path.exists(local_path):
        print(f"  [x] File not found: {local_path}")
        return False

    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"  Uploading {local_path} -> {r2_key} ({size_mb:.1f} MB)...")

    # Versioned archive upload
    if versioned:
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = os.path.basename(r2_key)
        # Derive archive prefix from the r2_key directory
        key_dir = os.path.dirname(r2_key)
        archive_key = f"{key_dir}/archive/{date_str}/{filename}"
        print(f"  Archiving to {archive_key}...")

        # Archive upload (best-effort, don't block on failure)
        for attempt in range(2):
            if _run_wrangler_upload(local_path, archive_key):
                print(f"  [ok] Archived {archive_key}")
                break
            if attempt == 0:
                print(f"  Retrying archive upload...")
        else:
            print(f"  [warn] Archive upload failed, continuing with latest...")

    # Latest upload (this is the critical one)
    for attempt in range(2):
        if _run_wrangler_upload(local_path, r2_key, cache_control):
            print(f"  [ok] Uploaded {r2_key}")
            return True
        if attempt == 0:
            print(f"  Retrying upload...")

    print(f"  [x] Upload failed after 2 attempts: {r2_key}")
    return False


def main():
    parser = argparse.ArgumentParser(description="Upload pipeline outputs to R2")
    parser.add_argument("--file", help="Specific file to upload")
    parser.add_argument("--dest", help="R2 destination key (used with --file)")
    parser.add_argument("--all", action="store_true", help="Upload all known outputs")
    parser.add_argument("--versioned", action="store_true",
                        help="Also archive a dated copy under archive/{date}/")
    args = parser.parse_args()

    if args.file:
        dest = args.dest or os.path.basename(args.file)
        success = upload_file(args.file, dest, versioned=args.versioned)
        sys.exit(0 if success else 1)

    if args.all:
        successes = 0
        failures = 0
        for filename, r2_key in UPLOAD_MAP.items():
            local_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.exists(local_path):
                if upload_file(local_path, r2_key, versioned=args.versioned):
                    successes += 1
                else:
                    failures += 1
            else:
                print(f"  - Skipping {filename} (not found)")

        print(f"\nDone: {successes} uploaded, {failures} failed")
        sys.exit(0 if failures == 0 else 1)

    parser.print_help()


if __name__ == "__main__":
    main()
