"""Recompress SNAPPY parquets in R2 to ZSTD, in place, without regenerating data.

Scope: */sat_dpto/*.parquet, */flood_dpto/*.parquet, data/eudr/hires/{combined,plantation}.
These predate the ZSTD write-time fix (parquet_io.write_h3_parquet, 2026-07) or were
re-inflated by writers that omitted COMPRESSION zstd (fixed alongside this script).

Per object: GET -> save original to the local backup dir -> read with pyarrow
(pandas would coerce nullable int columns to float; pyarrow keeps the exact schema)
-> sort by h3index + ZSTD-9 + 50K row groups (same encoding as optimize_parquets.py)
-> assert same rows/schema on re-read -> PUT back to the same key.

Idempotent: objects already ZSTD are logged and skipped; processed keys land in
_done.jsonl inside the backup dir, so an interrupted run resumes where it left off.
Deliberately does NOT go through upload_to_r2.py: that writer keeps */archive/
pre-overwrite copies in R2, which would add back the very bytes being reclaimed
(the local backup dir plays that role instead).

Usage:
  python pipeline/r2_recompress.py [--dry-run] [--workers 6] [--backup-dir DIR]
"""
import argparse
import io
import json
import os
import sys
import threading
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from r2_cleanup import ACCT, BUCKET, token, list_all

API = f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/r2/buckets/{BUCKET}/objects/"
DEFAULT_BACKUP = os.path.expanduser("~/r2-snappy-backup-2026-08")
EUDR_KEYS = {
    "data/eudr/hires/eudr_res9_combined.parquet",
    "data/eudr/hires/eudr_plantation_res9.parquet",
}

lock = threading.Lock()


def in_scope(key: str) -> bool:
    if key in EUDR_KEYS:
        return True
    return key.endswith(".parquet") and ("/sat_dpto/" in key or "/flood_dpto/" in key)


def http(method: str, key: str, tok: str, body: bytes = None) -> bytes:
    req = urllib.request.Request(
        API + urllib.parse.quote(key), method=method, data=body,
        headers={"Authorization": f"Bearer {tok}",
                 **({"Content-Type": "application/octet-stream"} if body else {})})
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read()


def recompress(raw: bytes):
    """Returns (new_bytes, n_rows) or (None, n_rows) if already fully ZSTD."""
    f = pq.ParquetFile(io.BytesIO(raw))
    codecs = {f.metadata.row_group(g).column(c).compression
              for g in range(f.metadata.num_row_groups)
              for c in range(f.metadata.num_columns)}
    if codecs == {"ZSTD"}:
        return None, f.metadata.num_rows
    table = f.read()
    if "h3index" in table.column_names:
        table = table.sort_by("h3index")
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=9,
                   row_group_size=50000)
    out = buf.getvalue()
    check = pq.read_table(io.BytesIO(out))
    assert check.num_rows == table.num_rows, "row count changed"
    assert check.schema.equals(table.schema, check_metadata=False), "schema changed"
    return out, table.num_rows


def process(key: str, size: int, tok: str, backup_dir: str, dry: bool):
    try:
        raw = http("GET", key, tok)
        if len(raw) != size:
            return (key, "error", 0, 0, f"size mismatch on GET ({len(raw)} vs {size})")
        bak = os.path.join(backup_dir, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(bak), exist_ok=True)
        if not os.path.exists(bak):
            with open(bak, "wb") as fh:
                fh.write(raw)
        new, _ = recompress(raw)
        if new is None:
            return (key, "already-zstd", size, size, "")
        if len(new) >= len(raw):
            return (key, "not-smaller", size, size, "")
        if not dry:
            for attempt in range(3):
                try:
                    http("PUT", key, tok, new)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise RuntimeError(f"PUT failed 3x: {e}")
        return (key, "dry-run" if dry else "ok", size, len(new), "")
    except Exception as e:
        return (key, "error", size, size, str(e)[:200])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="download+recompress, no PUT")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--backup-dir", default=DEFAULT_BACKUP)
    args = ap.parse_args()

    tok = token()
    keys = [(k, s) for k, s in list_all(tok) if in_scope(k)]
    os.makedirs(args.backup_dir, exist_ok=True)
    done_path = os.path.join(args.backup_dir, "_done.jsonl")
    done = set()
    if os.path.exists(done_path):
        for line in open(done_path, encoding="utf-8"):
            rec = json.loads(line)
            if rec["status"] in ("ok", "already-zstd", "not-smaller"):
                done.add(rec["key"])
    todo = [(k, s) for k, s in keys if k not in done]
    print(f"in scope: {len(keys):,} objects ({sum(s for _, s in keys)/2**20:.0f} MB)"
          f" | already done: {len(done):,} | to process: {len(todo):,}"
          + (" [DRY-RUN]" if args.dry_run else ""))

    stats = {"before": 0, "after": 0, "n": 0, "err": 0}
    with open(done_path, "a", encoding="utf-8") as log, \
            ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(process, k, s, tok, args.backup_dir, args.dry_run)
                for k, s in todo]
        for i, fut in enumerate(futs, 1):
            key, status, before, after, err = fut.result()
            with lock:
                if status == "error":
                    stats["err"] += 1
                    print(f"  ERR {key}: {err}")
                else:
                    stats["n"] += 1
                    stats["before"] += before
                    stats["after"] += after
                    if not args.dry_run:
                        log.write(json.dumps({"key": key, "status": status,
                                              "before": before, "after": after}) + "\n")
                        log.flush()
                if i % 200 == 0 or i == len(futs):
                    pct = 100 * stats["after"] / stats["before"] if stats["before"] else 0
                    print(f"  {i}/{len(futs)} | {stats['before']/2**20:.0f} MB -> "
                          f"{stats['after']/2**20:.0f} MB ({pct:.0f}%) | errors {stats['err']}")

    saved = (stats["before"] - stats["after"]) / 2**20
    print(f"\ndone: {stats['n']:,} processed, {stats['err']} errors, "
          f"{saved:.0f} MB reclaimed. Backup: {args.backup_dir}")
    return 1 if stats["err"] else 0


if __name__ == "__main__":
    sys.exit(main())
