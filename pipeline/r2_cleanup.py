"""R2 cleanup: delete ONLY objects the repo demonstrably never asks for.

Dry-run by default. Pass --execute to actually delete.

Deletes, in order of precedence:
  1. archive dirs: any key under '*/archive/' — written on purpose by upload_to_r2.py
     as a pre-overwrite backup, so this is a rollback-capability tradeoff, not junk.
  2. Prefix-orphans: keys whose top-level segment is neither 'data' nor 'tiles'
     (uploaded without the required data/ prefix → the CDN never serves them).
  3. Unreferenced .pmtiles: tiles no file in the repo names.

NEVER touches: tiles/lulc/** (raster land-use), temporal_baseline, or anything whose
key or filename appears anywhere under src/, pipeline/, functions/, db/ or .github/.

## Why the keep-set is derived, not written down

This script used to carry a hardcoded `KEEP` of 14 tiles "grepped from src/". The set
froze; the codebase did not. By 2026-07 it still protected parana_br_buildings-v4 while
config.ts had moved to -v8, and it had never heard of the 17 Paraguayan territories. Any
tile missing from that stale list was classified "stale building-tile version" — so
`--execute` would have deleted ~3.3 GB of LIVE building tiles (all of Brazil, Chaco,
Formosa, every PY department) and broken the buildings layer across half the map.

It also hardcoded `data/h3_radio_crosswalk_areal.parquet` as "unreferenced" — while
three CI workflows (land-use, deforestation, pm25) fetch exactly that key from R2 to
seed pipeline/output/. Deleting it would have broken them.

Both failures share one cause: an allowlist maintained by hand, out of band from the
code that does the referencing. So the keep-set is now scanned from the repo on every
run, and `assert_safe()` re-checks each candidate against that scan before any DELETE.
Add a territory, bump a tile version, wire a new CI fetch — this keeps up on its own.
"""
import os
import re
import sys
import json
import pathlib
import collections
import urllib.request
import urllib.parse

ACCT = "85b5bfbd1b86ba164b9443b87eefa3b8"
BUCKET = "neahub"
REPO = pathlib.Path(__file__).resolve().parent.parent

# Everything we ship or run. .github matters: CI fetches R2 keys by literal path.
SCAN_DIRS = ("src", "pipeline", "functions", "db", ".github")
SCAN_EXTS = {".ts", ".svelte", ".js", ".mjs", ".py", ".yml", ".yaml", ".json", ".toml", ".sh"}

BUILDING_RE = re.compile(r"^(data/)?tiles/.*\.pmtiles$")
FREE_TIER = 10
# Cloudflare never documents whether its "10 GB-month" tier is decimal or binary, and the
# API only returns bytes. Observed: at 10.745e9 the dashboard projects $0.00 with R2 in the
# filter — consistent with GiB, not with decimal. Reported both ways in r2_inventory.py;
# here it is only context for a size line, so binary (the conservative, lower reading).
GiB = 1024**3
MB = 1024**2


def repo_blob() -> str:
    """Concatenate every file that could name an R2 key. This is the source of truth
    for 'is this used', replacing the old hand-maintained allowlist."""
    me = pathlib.Path(__file__).resolve()
    parts = []
    for sub in SCAN_DIRS:
        d = REPO / sub
        if not d.exists():
            continue
        for p in d.rglob("*"):
            # Skip self: this file's docstring names keys as examples, and a script that
            # protects an object merely because it documents it protects nothing.
            if p.resolve() == me:
                continue
            if p.is_file() and p.suffix in SCAN_EXTS:
                try:
                    parts.append(p.read_text(encoding="utf-8", errors="ignore"))
                except OSError:
                    pass
    if not parts:
        raise SystemExit("refusing to run: scanned no files, keep-set would be empty")
    return "\n".join(parts)


def referenced(key: str, blob: str) -> bool:
    """True if the repo names this object, by full key or by bare filename.
    Filename alone counts: CI writes `neahub/data/x.parquet`, config.ts writes
    `data/x.parquet`, a script may write just `x.parquet` — all mean 'live'."""
    return key in blob or key.split("/")[-1] in blob


def reason(key: str, blob: str):
    # Raster land-use pyramid: thousands of keys, never named individually.
    if key.startswith("tiles/lulc/"):
        return None
    # Structural categories first — these are unreachable regardless of naming.
    if "/archive/" in key or key.startswith("data/archive/") or key.startswith("tiles/archive"):
        return "archive dir (pre-overwrite backup)"
    top = key.split("/")[0]
    if top not in ("data", "tiles"):
        return "prefix-orphan (no data/ prefix → never served)"
    # Everything else: the repo decides.
    if referenced(key, blob):
        return None
    if BUILDING_RE.match(key):
        return "unreferenced tile"
    return None


# ── Unserved parquets: report, never delete ─────────────────────────────────
# reason() only ever classifies .pmtiles as unreferenced; for a parquet it returns
# None. That is deliberate — per-department parquets are never named literally
# (config.ts builds them as `sat_${analysisId}_${dept}.parquet`), so a naive
# "unreferenced parquet" rule would propose deleting ~12.7k LIVE files.
#
# The cost of that silence: the 5 composites pruned on 2026-05-31 sat in R2 for six
# weeks (1.002 objs, 362 MB) while this script printed "Nothing to delete".
#
# So we judge parquets by a different question — not "does any file name it?" but
# "does the SITE build a URL for it?" — and we only REPORT the answer. These are not
# auto-deletable: some unserved parquets are CI inputs that workflows fetch from R2 by
# shell-variable name (`r2 object get "neahub/data/${f}.parquet"`), which no literal
# scan can see. Deleting those breaks the pipeline. A human decides; this just points.

SITE_DIR = "src"

# Per-department URL shapes, straight from config.ts:116/120/124.
DEPT_DIRS = {"flood_dpto": "hex_flood_", "scores_dpto": "overture_scores_", "sat_dpto": "sat_"}


def site_blob() -> str:
    """Only src/ — the serving surface. A name that lives solely in pipeline/ means
    'we can generate it', not 'we serve it'; that is the whole distinction here."""
    parts = []
    for p in (REPO / SITE_DIR).rglob("*"):
        if p.is_file() and p.suffix in SCAN_EXTS:
            try:
                parts.append(p.read_text(encoding="utf-8", errors="ignore"))
            except OSError:
                pass
    if not parts:
        raise SystemExit("refusing to run: scanned no src/ files")
    return "\n".join(parts)


def served_stems(sblob: str) -> set:
    """Parquet stems the site builds a URL for: getParquetUrl('x') -> /data/x.parquet,
    the registry's `parquet:` field (also the sat_dpto prefix), and literal /data/x.parquet."""
    out = set()
    out |= set(re.findall(r"getParquetUrl\('([a-z0-9_]+)'\)", sblob))
    out |= set(re.findall(r"parquet:\s*'([a-z0-9_]+)'", sblob))
    out |= set(re.findall(r"/data/([a-z0-9_]+)\.parquet", sblob))
    return {s for s in out if s}


def served(key: str, stems: set) -> bool:
    parts = key.split("/")
    stem = parts[-1][: -len(".parquet")]
    if stem in stems:
        return True
    d = parts[-2] if len(parts) > 1 else ""
    if d in DEPT_DIRS and stem.startswith(DEPT_DIRS[d]):
        # sat_dpto is layer-scoped: sat_<layer>_<dept> is live only if <layer> is served.
        # flood_dpto/scores_dpto are whole-surface: the dir itself is what the site asks for.
        if d == "sat_dpto":
            return any(stem.startswith(s + "_") for s in stems)
        return True
    return any(stem.startswith(s + "_") for s in stems)


def report_unserved(keys, stems, blob):
    fam = collections.defaultdict(lambda: [0, 0, False])
    for k, s in keys:
        if not k.endswith(".parquet") or "/archive/" in k:
            continue
        if served(k, stems):
            continue
        stem = k.split("/")[-1][: -len(".parquet")]
        f = "_".join(stem.split("_")[:3])
        fam[f][0] += 1
        fam[f][1] += s
        fam[f][2] = fam[f][2] or referenced(k, blob)  # named outside src/ => maybe a CI input
    if not fam:
        print("\nUnserved parquets: none — every parquet in R2 maps to a URL src/ builds.")
        return
    tot = sum(v[1] for v in fam.values())
    print(f"\n!! {sum(v[0] for v in fam.values()):,} parquet(s) that src/ never builds a URL for"
          f"  ({tot/MB:.1f} MB) — REVIEW BY HAND, not deleted by this script:")
    for f, (c, b, ci) in sorted(fam.items(), key=lambda x: -x[1][1]):
        note = "pipeline/CI names it -> may be a CI input, check before deleting" if ci else "nothing names it"
        print(f"   {f:34s} {c:5,d} obj  {b/MB:8.1f} MB   [{note}]")


def assert_safe(todel, blob):
    """Last line of defence: nothing the repo names may reach a DELETE. Archive dirs are
    exempt — their filenames legitimately match live parquets by construction."""
    violations = [
        (k, r) for k, _, r in todel
        if not r.startswith("archive dir") and referenced(k, blob)
    ]
    if violations:
        print("\n!!! ABORT: candidates that the repo still references:")
        for k, r in violations[:20]:
            print(f"    [{r}] {k}")
        raise SystemExit("refusing to delete referenced objects")


def token():
    if os.environ.get("CLOUDFLARE_API_TOKEN"):
        return os.environ["CLOUDFLARE_API_TOKEN"]
    cfg = os.path.expanduser("~/.wrangler/config/default.toml")
    for line in open(cfg, encoding="utf-8"):
        m = re.match(r'^oauth_token\s*=\s*"(.*)"', line.strip())
        if m:
            return m.group(1)
    raise SystemExit("no oauth_token — run `npx wrangler whoami` first")


def list_all(tok):
    base = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects"
    cur, keys = None, []
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
    req = urllib.request.Request(base + urllib.parse.quote(key), method="DELETE",
                                 headers={"Authorization": f"Bearer {tok}"})
    try:
        urllib.request.urlopen(req, timeout=60)
        return True
    except Exception as e:
        print(f"    ERR deleting {key}: {e}")
        return False


def main():
    execute = "--execute" in sys.argv
    # Categories are opt-in: without a flag, only the structurally-unreachable ones run.
    want_archive = "--archives" in sys.argv

    blob = repo_blob()
    live_tiles = set(re.findall(r"((?:data/)?tiles/[A-Za-z0-9_\-./]+\.pmtiles)", blob))
    print(f"keep-set scanned from repo: {len(live_tiles)} tiles referenced")

    tok = token()
    keys = list_all(tok)
    total = sum(s for _, s in keys)

    todel = []
    for k, s in keys:
        r = reason(k, blob)
        if not r:
            continue
        if r.startswith("archive dir") and not want_archive:
            continue
        todel.append((k, s, r))

    # A tile the code asks for but R2 lacks means the map is already broken — say so.
    r2_tiles = {k for k, _ in keys if k.endswith(".pmtiles")}
    missing = live_tiles - r2_tiles
    if missing:
        print(f"\n!! {len(missing)} tile(s) referenced by the repo are ABSENT from R2:")
        for m in sorted(missing):
            print("   ", m)

    over = total / GiB - FREE_TIER
    print(f"\nCurrent: {len(keys):,} objects  {total/GiB:.3f} GiB"
          f"  [{'+' if over > 0 else ''}{over:.3f} vs a 10-GiB tier; see r2_inventory.py"
          f" for both readings]")
    # Before any early return: "nothing to delete" must never again mean "nothing to see".
    report_unserved(keys, served_stems(site_blob()), blob)

    if not todel:
        print("\nNothing to delete (in the categories this script deletes).")
        return

    by_cat = {}
    for k, s, r in todel:
        by_cat.setdefault(r, [0, 0])
        by_cat[r][0] += 1
        by_cat[r][1] += s
    print(f"\n=== TO DELETE: {len(todel):,} objects  {sum(s for _, s, _ in todel)/MB:.1f} MB ===")
    for r, (c, b) in sorted(by_cat.items(), key=lambda x: -x[1][1]):
        print(f"  {r:48s} {c:6,d} obj  {b/MB:9.1f} MB")
    print(f"\nProjected after: {(total - sum(s for _, s, _ in todel))/GiB:.3f} GiB")

    print("\n=== non-archive deletions (review individually) ===")
    shown = [t for t in sorted(todel) if not t[2].startswith("archive dir")]
    for k, s, r in shown[:40]:
        print(f"  [{r[:26]:26s}] {k}  {s/MB:.1f}MB")
    if not shown:
        print("  (none)")

    assert_safe(todel, blob)

    if not execute:
        print("\nDRY-RUN. Re-run with --execute to delete."
              + ("" if want_archive else "  (--archives to include archive dirs)"))
        return

    print(f"\n=== DELETING {len(todel):,} objects ===")
    ok = 0
    for i, (k, _, _) in enumerate(todel):
        if delete(tok, k):
            ok += 1
        if (i + 1) % 50 == 0:
            print(f"  ...{i+1}/{len(todel)}")
    print(f"Deleted {ok}/{len(todel)}")


if __name__ == "__main__":
    main()
