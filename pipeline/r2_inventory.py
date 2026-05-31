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


def token():
    cfg = os.path.expanduser("~/.wrangler/config/default.toml")
    for line in open(cfg, encoding="utf-8"):
        m = re.match(r'^oauth_token\s*=\s*"(.*)"', line.strip())
        if m:
            return m.group(1)
    raise SystemExit("no oauth_token in wrangler config")


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

    GB = 1024**3
    MB = 1024**2
    print(f"\n=== TOTAL: {total_n:,} objects  {total_b/GB:.2f} GB ===\n")
    print("=== by top-level prefix ===")
    for p, (c, b) in sorted(by_prefix.items(), key=lambda x: -x[1][1]):
        print(f"  {p:30s} {c:8,d} obj  {b/GB:7.3f} GB")
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
    print(f"\n  >>> reclaimable from stale versioned files: {waste/MB:.1f} MB ({waste/GB:.3f} GB)")


if __name__ == "__main__":
    main()
