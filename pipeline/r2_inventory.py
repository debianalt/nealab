"""R2 inventory: paginate all objects, aggregate by top-level prefix, and flag
versioned duplicate families (foo-v1/foo-v2/foo). Read-only. Helps plan cleanup
to stay under the 10 GB free tier."""
import os
import re
import sys
import json
import subprocess
import urllib.request
import collections

ACCT = "85b5bfbd1b86ba164b9443b87eefa3b8"
BUCKET = "neahub"


def _read_oauth(cfg):
    if not os.path.exists(cfg):
        return None
    for line in open(cfg, encoding="utf-8"):
        m = re.match(r'^oauth_token\s*=\s*"(.*)"', line.strip())
        if m:
            return m.group(1)
    return None


def token():
    # 1) Prefer a persistent API token (CLOUDFLARE_API_TOKEN) — it never expires.
    env = os.environ.get("CLOUDFLARE_API_TOKEN")
    if env:
        return env.strip()
    # 2) Fall back to wrangler's OAuth token. It has an expiration_time and a stale
    #    cached value 401s — so first run `wrangler whoami`, which transparently
    #    refreshes it via the refresh_token. This makes the script self-heal instead
    #    of throwing the recurring "need a new token" 401.
    cfg = os.path.expanduser("~/.wrangler/config/default.toml")
    try:
        subprocess.run(["npx", "wrangler", "whoami"], capture_output=True,
                       timeout=90, shell=(os.name == "nt"))
    except Exception:
        pass
    tok = _read_oauth(cfg)
    if tok:
        return tok
    raise SystemExit(
        "No Cloudflare credential found. Either set CLOUDFLARE_API_TOKEN (a no-expiry "
        "API token with R2 read) or run `npx wrangler login`."
    )


def main():
    tok = token()
    base = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects"
    cursor = None
    total_n = 0
    total_b = 0
    by_prefix = collections.defaultdict(lambda: [0, 0])  # prefix -> [count, bytes]
    by_dir = collections.defaultdict(lambda: [0, 0])      # 2-level dir -> [count, bytes]
    all_keys = []  # (key, size)
    while True:
        url = base + "?per_page=1000"
        if cursor:
            url += "&cursor=" + urllib.parse.quote(cursor)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        d = json.load(urllib.request.urlopen(req, timeout=60))
        res = d.get("result", [])
        for o in res:
            k = o["key"]
            s = int(o["size"])
            total_n += 1
            total_b += s
            top = k.split("/")[0]
            by_prefix[top][0] += 1
            by_prefix[top][1] += s
            parts = k.split("/")
            dkey = "/".join(parts[:2]) if len(parts) > 1 else parts[0]
            by_dir[dkey][0] += 1
            by_dir[dkey][1] += s
            all_keys.append((k, s))
        info = d.get("result_info", {})
        cursor = info.get("cursor")
        if not info.get("is_truncated") or not cursor:
            break
        print(f"  ...{total_n} objects so far", file=sys.stderr)

    # Is the "10 GB-month" free tier decimal or binary? Cloudflare's pricing page states
    # "10 GB-month / month" and "$0.015 / GB-month" and never defines GB; the API only
    # ever returns bytes. The observable evidence favours binary: at 10.745e9 bytes the
    # dashboard projects $0.00 with R2 inside the product filter, which fits a GiB tier
    # (0.007 over → $0.0001/mo) and not a decimal one (0.745 over → $0.011/mo, which
    # would render as $0.01).
    #
    # So print both and let Billing be the authority. Do NOT encode a guess: a previous
    # pass rewrote this to decimal on assumption alone and reported an overage 100x the
    # bill. Whichever it is, the number here is ~1 cent — this script is for spotting
    # junk, not for costing.
    GiB = 1024**3
    GBd = 1000**3
    MB = 1024**2
    FREE_TIER = 10
    ovr_bin = max(total_b / GiB - FREE_TIER, 0)
    ovr_dec = max(total_b / GBd - FREE_TIER, 0)
    # ASCII only: this runs in a cp1252 console on Windows, where a stray arrow or em
    # dash raises UnicodeEncodeError and kills the report mid-print.
    print(f"\n=== TOTAL: {total_n:,} objects ===")
    print(f"    {total_b/GiB:7.3f} GiB binary   -> {ovr_bin:5.3f} over a 10-GiB tier = ${ovr_bin*0.015:.4f}/mo")
    print(f"    {total_b/GBd:7.3f} GB  decimal  -> {ovr_dec:5.3f} over a 10-GB  tier = ${ovr_dec*0.015:.4f}/mo")
    print("    Unit undocumented; dashboard Billing decides. Billing basis is the average")
    print("    of PEAK DAILY storage across the cycle, not this snapshot.")
    print()
    print("=== by top-level prefix ===")
    for p, (c, b) in sorted(by_prefix.items(), key=lambda x: -x[1][1]):
        print(f"  {p:30s} {c:8,d} obj  {b/GiB:7.3f} GiB")
    print("\n=== by 2-level dir (top 40 by size) ===")
    for p, (c, b) in sorted(by_dir.items(), key=lambda x: -x[1][1])[:40]:
        print(f"  {p:45s} {c:7,d} obj  {b/MB:9.1f} MB")

    # versioned-duplicate families: strip -vN before extension
    fam = collections.defaultdict(list)
    vpat = re.compile(r"^(.*?)(?:-v(\d+))?(\.[a-z]+)$")
    for k, s in all_keys:
        m = vpat.match(k)
        if m and m.group(2) is not None:
            fam[m.group(1) + m.group(3)].append((int(m.group(2)), k, s))
    print("\n=== versioned families with >1 version (potential stale to delete) ===")
    waste = 0
    for base_k, vs in sorted(fam.items(), key=lambda x: -sum(v[2] for v in x[1])):
        if len(vs) > 1:
            vs.sort()
            keep = vs[-1]
            stale = vs[:-1]
            sb = sum(v[2] for v in stale)
            waste += sb
            print(f"  {base_k}: versions {[v[0] for v in vs]} keep v{keep[0]} "
                  f"| stale {sb/MB:.1f} MB")
    print(f"\n  >>> reclaimable from stale versioned files: {waste/MB:.1f} MB ({waste/GiB:.3f} GiB)")


if __name__ == "__main__":
    main()
