"""Upload a large file to R2 via the Cloudflare REST API multipart flow,
using the existing wrangler OAuth token (no S3 access keys needed). The
single-PUT REST endpoint caps at ~300 MB; multipart (mpu-create / mpu-uploadpart
/ mpu-complete) lifts that. Parts must be equal size except the last, >=5 MiB.

Usage: python r2_upload_multipart.py <r2-key> <local-file> [part_mb]
"""
import os
import re
import sys
import json
import urllib.request
import urllib.parse

ACCT = "85b5bfbd1b86ba164b9443b87eefa3b8"
BUCKET = "neahub"


def token():
    cfg = os.path.expanduser("~/.wrangler/config/default.toml")
    for line in open(cfg, encoding="utf-8"):
        m = re.match(r'^oauth_token\s*=\s*"(.*)"', line.strip())
        if m:
            return m.group(1)
    raise SystemExit("no oauth_token")


def req(method, url, tok, data=None, ctype=None):
    h = {"Authorization": f"Bearer {tok}"}
    if ctype:
        h["Content-Type"] = ctype
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(r, timeout=600) as resp:
        body = resp.read()
    try:
        return json.loads(body)
    except Exception:
        return {"_raw": body}


def main():
    key, path = sys.argv[1], sys.argv[2]
    part_mb = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    tok = token()
    base = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects/{key}"
    size = os.path.getsize(path)
    part = part_mb * 1024 * 1024
    nparts = (size + part - 1) // part
    print(f"file {size/1048576:.1f} MB -> {nparts} parts of {part_mb} MB")

    d = req("POST", base + "?action=mpu-create", tok)
    if not d.get("success"):
        raise SystemExit(f"mpu-create failed: {d}")
    upload_id = d["result"]["uploadId"]
    print(f"uploadId={upload_id}")

    parts = []
    try:
        with open(path, "rb") as f:
            for i in range(1, nparts + 1):
                chunk = f.read(part)
                u = base + "?action=mpu-uploadpart&uploadId=" + urllib.parse.quote(upload_id) + f"&partNumber={i}"
                pr = req("PUT", u, tok, data=chunk, ctype="application/octet-stream")
                if not pr.get("success"):
                    raise SystemExit(f"part {i} failed: {pr}")
                etag = pr["result"]["etag"]
                parts.append({"partNumber": i, "etag": etag})
                print(f"  part {i}/{nparts} ok ({len(chunk)/1048576:.0f} MB) etag={etag[:16]}")
        body = json.dumps({"parts": parts}).encode()
        cu = base + "?action=mpu-complete&uploadId=" + urllib.parse.quote(upload_id)
        cr = req("POST", cu, tok, data=body, ctype="application/json")
        if not cr.get("success"):
            raise SystemExit(f"mpu-complete failed: {cr}")
        print("COMPLETE:", cr["result"].get("key", key))
    except Exception:
        au = base + "?action=mpu-abort&uploadId=" + urllib.parse.quote(upload_id)
        try:
            req("DELETE", au, tok)
            print("aborted mpu")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
