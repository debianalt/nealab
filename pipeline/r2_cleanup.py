"""R2 cleanup: delete ONLY verified-redundant/orphan objects to stay under the
10 GB free tier. Conservative — explicit categories, hard allowlist, dry-run by
default. Pass --execute to actually delete.

Deletes:
  1. Stale building-tile versions: (data/)tiles/*_buildings(-vN).pmtiles NOT in the
     live keep-set grepped from src/ (the 14 referenced tiles).
  2. tiles/buildings-vN.pmtiles older than the live buildings-v5.
  3. tiles/radios-v2.pmtiles (legacy; live is data/tiles/radios-v3).
  4. archive dirs: any key with '/archive/' or starting 'tiles/archive'/'data/archive'.
  5. Prefix-orphans: keys whose top-level segment is neither 'data' nor 'tiles'
     (uploaded without the required data/ prefix → never served).
  6. data/h3_radio_crosswalk_areal.parquet (superseded by dasymetric; unreferenced).

NEVER touches: the 14 live tiles, tiles/lulc/** (raster land-use), tiles/hexagons-v2,
tiles/catastro.pmtiles, temporal_baseline, or any per-territory live parquet/sat_dpto.
"""
import os
import re
import sys
import json
import urllib.request
import urllib.parse

ACCT = "85b5bfbd1b86ba164b9443b87eefa3b8"
BUCKET = "neahub"

# 14 live tiles grepped from src/ — the hard keep-set
KEEP = {
    "data/tiles/alto_parana_buildings-v4.pmtiles", "data/tiles/alto_parana_districts.pmtiles",
    "data/tiles/chaco_buildings-v3.pmtiles", "data/tiles/corrientes_buildings-v4.pmtiles",
    "data/tiles/formosa_buildings-v3.pmtiles", "data/tiles/itapua_districts.pmtiles",
    "data/tiles/parana_br_buildings-v4.pmtiles", "data/tiles/radios-v3.pmtiles",
    "data/tiles/rio_grande_sul_br_buildings-v4.pmtiles",
    "data/tiles/santa_catarina_br_buildings-v4.pmtiles",
    "tiles/buildings-v5.pmtiles", "tiles/catastro.pmtiles",
    "tiles/hexagons-v2.pmtiles", "tiles/itapua_buildings-v4.pmtiles",
}

BUILDING_RE = re.compile(r"^(data/)?tiles/.*buildings(-v\d+)?\.pmtiles$")
RADIOS_LEGACY_RE = re.compile(r"^tiles/radios(-v\d+)?\.pmtiles$")


def token():
    cfg = os.path.expanduser("~/.wrangler/config/default.toml")
    for line in open(cfg, encoding="utf-8"):
        m = re.match(r'^oauth_token\s*=\s*"(.*)"', line.strip())
        if m:
            return m.group(1)
    raise SystemExit("no oauth_token")


def reason(key):
    if key in KEEP:
        return None
    # never touch raster land-use tiles
    if key.startswith("tiles/lulc/"):
        return None
    top = key.split("/")[0]
    if top not in ("data", "tiles"):
        return "prefix-orphan (no data/ prefix)"
    if "/archive/" in key or key.startswith("tiles/archive") or key.startswith("data/archive/"):
        return "archive dir"
    if BUILDING_RE.match(key):
        return "stale building-tile version"
    if RADIOS_LEGACY_RE.match(key):
        return "legacy radios tile (live=data/tiles/radios-v3)"
    if key == "data/h3_radio_crosswalk_areal.parquet":
        return "orphan areal crosswalk (unreferenced)"
    return None


def list_all(tok):
    base = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects"
    cur = None
    keys = []
    while True:
        u = base + "?per_page=1000"
        if cur:
            u += "&cursor=" + urllib.parse.quote(cur)
        d = json.load(urllib.request.urlopen(
            urllib.request.Request(u, headers={"Authorization": f"Bearer {tok}"}), timeout=60))
        for o in d.get("result", []):
            keys.append((o["key"], int(o["size"])))
        i = d.get("result_info", {})
        cur = i.get("cursor")
        if not i.get("is_truncated") or not cur:
            break
    return keys


def delete(tok, key):
    base = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects/"
    u = base + urllib.parse.quote(key)
    req = urllib.request.Request(u, method="DELETE", headers={"Authorization": f"Bearer {tok}"})
    try:
        urllib.request.urlopen(req, timeout=60)
        return True
    except Exception as e:
        print(f"    ERR deleting {key}: {e}")
        return False


def main():
    execute = "--execute" in sys.argv
    tok = token()
    keys = list_all(tok)
    total = sum(s for _, s in keys)
    todel = [(k, s, reason(k)) for k, s in keys if reason(k)]
    by_cat = {}
    for k, s, r in todel:
        by_cat.setdefault(r, [0, 0])
        by_cat[r][0] += 1
        by_cat[r][1] += s
    MB = 1048576
    GB = 1024**3
    print(f"Current: {len(keys):,} objects  {total/GB:.3f} GB")
    print(f"\n=== TO DELETE: {len(todel):,} objects  {sum(s for _,s,_ in todel)/MB:.1f} MB ===")
    for r, (c, b) in sorted(by_cat.items(), key=lambda x: -x[1][1]):
        print(f"  {r:45s} {c:6,d} obj  {b/MB:9.1f} MB")
    print(f"\nProjected after: {(total - sum(s for _,s,_ in todel))/GB:.3f} GB")
    # show the building-tile + legacy + areal deletions explicitly (the risky-looking ones)
    print("\n=== explicit non-archive/non-orphan deletions (review) ===")
    for k, s, r in sorted(todel):
        if r not in ("prefix-orphan (no data/ prefix)", "archive dir"):
            print(f"  [{r[:20]:20s}] {k}  {s/MB:.1f}MB")
    if not execute:
        print("\nDRY-RUN. Re-run with --execute to delete.")
        return
    print(f"\n=== DELETING {len(todel):,} objects ===")
    ok = 0
    for i, (k, s, r) in enumerate(todel):
        if delete(tok, k):
            ok += 1
        if (i + 1) % 50 == 0:
            print(f"  ...{i+1}/{len(todel)}")
    print(f"Deleted {ok}/{len(todel)}")


if __name__ == "__main__":
    main()
