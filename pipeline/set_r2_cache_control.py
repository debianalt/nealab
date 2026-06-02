"""
Set Cache-Control: public, max-age=86400, immutable on all parquet objects in R2.
Uses wrangler r2 object put with --copy-source-metadata to update headers without re-uploading.
Since wrangler doesn't support metadata-only updates, we download and re-upload with correct headers.

Usage:
    python pipeline/set_r2_cache_control.py --dry-run   # show what would be updated
    python pipeline/set_r2_cache_control.py              # actually update

This script collects all parquet URLs from config data and updates their Cache-Control.
Priority: per-dept parquets for the most-used territories (fast UX impact).
"""
import subprocess, sys, os, json

R2_BUCKET = "neahub"
CACHE_CONTROL = "public, max-age=86400, immutable"

# Most-accessed territories (per-dept parquets are the hot path for users)
TERRITORIES_AND_ANALYSES = [
    # (prefix, analyses, summary_pattern)
    ("", ["agri_potential","forestry_aptitude","carbon_stock","deforestation_dynamics","pm25_drivers","land_use","accessibility","flood_risk"], "sat_{a}_dept_summary"),
    ("itapua_py/", ["agri_potential","forestry_aptitude","carbon_stock","deforestation_dynamics","pm25_drivers","land_use","accessibility"], "itapua_py_sat_{a}_summary"),
    ("alto_parana_py/", ["agri_potential","forestry_aptitude","carbon_stock","land_use"], "alto_parana_py_sat_{a}_summary"),
    ("corrientes/", ["agri_potential","forestry_aptitude","carbon_stock","pm25_drivers","land_use","accessibility"], "corrientes_sat_{a}_summary"),
]

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "lib", "data")

def get_parquet_keys(territory_prefix, analysis, summary_pattern):
    """Load dept summary JSON and extract parquetKeys."""
    if territory_prefix == "":
        # Misiones: summary file is sat_{analysis}_dept_summary.json
        fname = f"sat_{analysis}_dept_summary.json"
    elif summary_pattern.startswith("flood"):
        fname = summary_pattern.replace("{a}", analysis) + ".json"
    else:
        # Use pattern
        fname = summary_pattern.replace("{a}", analysis) + ".json"

    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    return [d.get("parquetKey") for d in data.get("departments", []) if d.get("parquetKey")]

def get_r2_key(territory_prefix, analysis, parquet_key):
    """Construct R2 key for a per-dept parquet."""
    if analysis == "flood_risk":
        folder = "flood_dpto"
        name = f"hex_flood_{parquet_key}.parquet"
    else:
        folder = "sat_dpto"
        name = f"sat_{analysis}_{parquet_key}.parquet"
    return f"data/{territory_prefix}{folder}/{name}"

def set_cache_control(r2_key, dry_run=False):
    """Re-upload R2 object with Cache-Control header (wrangler downloads+reuploads)."""
    if dry_run:
        print(f"  [DRY] would update: {r2_key}")
        return True

    # First download to a temp file
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    # Download
    dl = subprocess.run(
        ["npx", "wrangler", "r2", "object", "get", f"{R2_BUCKET}/{r2_key}",
         "--file", tmp_path, "--remote"],
        capture_output=True, shell=True, encoding="utf-8", errors="replace"
    )
    if dl.returncode != 0:
        print(f"  [x] download failed: {r2_key}")
        os.unlink(tmp_path)
        return False

    # Re-upload with Cache-Control
    ul = subprocess.run(
        ["npx", "wrangler", "r2", "object", "put", f"{R2_BUCKET}/{r2_key}",
         "--file", tmp_path, "--remote",
         "--cache-control", CACHE_CONTROL,
         "--content-type", "application/vnd.apache.parquet"],
        capture_output=True, shell=True, encoding="utf-8", errors="replace"
    )
    os.unlink(tmp_path)
    if ul.returncode == 0:
        print(f"  [ok] {r2_key}")
        return True
    else:
        print(f"  [x] upload failed: {r2_key}: {ul.stderr.strip()[:100]}")
        return False

def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("DRY RUN — no changes will be made\n")

    total = 0
    for prefix, analyses, pattern in TERRITORIES_AND_ANALYSES:
        terr_label = prefix or "misiones"
        print(f"\n=== Territory: {terr_label} ===")
        for analysis in analyses:
            keys = get_parquet_keys(prefix, analysis, pattern)
            if not keys:
                print(f"  [skip] no summary for {analysis}")
                continue
            print(f"  {analysis}: {len(keys)} depts")
            for key in keys:
                r2_key = get_r2_key(prefix, analysis, key)
                set_cache_control(r2_key, dry_run=dry_run)
                total += 1

    print(f"\nTotal processed: {total} parquets")

if __name__ == "__main__":
    main()
