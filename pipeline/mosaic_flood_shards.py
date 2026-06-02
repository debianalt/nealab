"""
Mosaic sharded flood GeoTIFF exports from GEE.
GEE splits large exports into numbered tiles; this merges them.

Usage: python pipeline/mosaic_flood_shards.py <territory_output_dir>
"""
import glob
import os
import sys

import rasterio
from rasterio.merge import merge


def mosaic_shards(t_dir: str, prefix: str, output_name: str) -> bool:
    pattern = os.path.join(t_dir, f"{prefix}*[0-9].tif")
    shards = sorted(glob.glob(pattern))
    out = os.path.join(t_dir, output_name)

    if not shards:
        print(f"  No shards found for {prefix}")
        return False

    if len(shards) == 1:
        import shutil
        shutil.copy2(shards[0], out)
        print(f"  Single shard → copied to {output_name}")
        return True

    print(f"  Mosaicking {len(shards)} shards → {output_name}")
    datasets = [rasterio.open(s) for s in shards]
    data, transform = merge(datasets)
    meta = datasets[0].meta.copy()
    meta.update(width=data.shape[2], height=data.shape[1], transform=transform, compress='lzw')
    with rasterio.open(out, 'w', **meta) as dst:
        dst.write(data)
    for d in datasets:
        d.close()
    print(f"  Done: {os.path.getsize(out) // (1024*1024)}MB")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python mosaic_flood_shards.py <territory_dir>")
        sys.exit(1)
    t_dir = sys.argv[1]
    print(f"Mosaicking flood shards in: {t_dir}")
    mosaic_shards(t_dir, "flood_recurrence_historical", "flood_recurrence_historical.tif")
    mosaic_shards(t_dir, "flood_current_", "flood_current_mosaic.tif")


if __name__ == "__main__":
    main()
