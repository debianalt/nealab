"""Apply frontend updates after v1.1 finalize runs:
  1. Add imports to TERRITORY_SUMMARIES maps in deptSummaries.ts for each
     <territory, layer> bundled summary JSON now present in src/lib/data/.
  2. Flip coverage entries in config.ts ANALYSIS_REGISTRY from 'unavailable'
     to 'available' for the (layer, territory) pairs whose summary JSON
     exists.
  3. Bump per-parquet cache busters in config.ts getParquetUrl() so
     DuckDB-WASM picks up the new R2 versions.

Driven by what's actually present on disk — won't flip coverage for a
layer × territory whose JSON didn't land. Idempotent.

Usage:
  python pipeline/apply_v11_frontend_updates.py
  python pipeline/apply_v11_frontend_updates.py --dry-run
"""
from __future__ import annotations
import argparse
import re
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DATA_DIR = REPO / "src" / "lib" / "data"
DEPT_SUMMARIES_TS = REPO / "src" / "lib" / "utils" / "deptSummaries.ts"
CONFIG_TS = REPO / "src" / "lib" / "config.ts"

NEW_TERRITORIES = ['chaco', 'formosa', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']

# Layers we may have generated in this v1.1 push. Each enables a `sat_<id>`
# parquet (territorial_scores has a different summary filename convention).
LAYERS = [
    'carbon_stock', 'pm25_drivers', 'productive_activity',
    'deforestation_dynamics', 'climate_vulnerability', 'soil_water',
    'territorial_scores', 'land_use', 'flood_risk',
]

# Per-territory const name in deptSummaries.ts
TERR_CONST = {
    'chaco': 'CHACO_SUMMARIES',
    'formosa': 'FORMOSA_SUMMARIES',
    'parana_br': 'PARANA_BR_SUMMARIES',
    'santa_catarina_br': 'SANTA_CATARINA_BR_SUMMARIES',
    'rio_grande_sul_br': 'RIO_GRANDE_SUL_BR_SUMMARIES',
}


def summary_filename(territory: str, layer: str) -> str:
    """Filename in src/lib/data for a given (territory, layer) bundled summary."""
    if layer == 'territorial_scores':
        return f"{territory}_scores_dept_summary.json"
    if layer == 'flood_risk':
        return f"{territory}_flood_dept_summary.json"
    return f"{territory}_sat_{layer}_summary.json"


def detect_present_pairs() -> dict[str, list[str]]:
    """Return {territory: [layers that have a bundled summary on disk]}."""
    out = {}
    for t in NEW_TERRITORIES:
        present = []
        for layer in LAYERS:
            f = DATA_DIR / summary_filename(t, layer)
            if f.exists():
                present.append(layer)
        out[t] = present
    return out


def patch_dept_summaries(present: dict[str, list[str]], dry_run: bool) -> int:
    """Insert import entries into each TERRITORY_SUMMARIES map in deptSummaries.ts."""
    src = DEPT_SUMMARIES_TS.read_text(encoding='utf-8')
    original = src
    changed = 0

    for terr, layers in present.items():
        const = TERR_CONST[terr]
        # Find the block "const <CONST>: ... = { ... };"
        pat = re.compile(
            rf"(const {const}: Record<string, \(\) => Promise<any>> = \{{)(.*?)(\}};)",
            re.DOTALL,
        )
        m = pat.search(src)
        if not m:
            print(f"  WARN: {const} block not found in deptSummaries.ts — skip {terr}")
            continue
        body = m.group(2)
        additions = []
        for layer in layers:
            # Skip if already present
            if re.search(rf"^\s*{layer}\s*:", body, re.MULTILINE):
                continue
            json_name = summary_filename(terr, layer)
            additions.append(f"\t{layer:<24}: () => import('$lib/data/{json_name}'),")
        if not additions:
            continue
        # Append before closing brace of the map (preserve trailing newline)
        new_body = body.rstrip() + "\n" + "\n".join(additions) + "\n"
        src = src.replace(m.group(0), m.group(1) + new_body + m.group(3))
        changed += len(additions)
        print(f"  + {const}: added {len(additions)} entries ({', '.join(layers)})")

    if dry_run:
        return changed
    if src != original:
        DEPT_SUMMARIES_TS.write_text(src, encoding='utf-8', newline='\n')
    return changed


def patch_coverage(present: dict[str, list[str]], dry_run: bool) -> int:
    """Flip coverage entries in config.ts ANALYSIS_REGISTRY: 'unavailable' → 'available'
    for each (layer, territory) pair present on disk."""
    src = CONFIG_TS.read_text(encoding='utf-8')
    original = src
    # Find ANALYSIS_REGISTRY block
    start = src.find('export const ANALYSIS_REGISTRY')
    end_marker = '// ── Territorial Scores definitions'
    end = src.find(end_marker)
    if start < 0 or end < 0:
        print("  WARN: could not locate ANALYSIS_REGISTRY bounds")
        return 0
    block = src[start:end]
    new_block = block
    flips = 0

    # For each layer, find its entry and flip the coverage entries per territory
    for layer in LAYERS:
        # Find entry block by id
        id_pat = re.compile(rf"(\{{\s*id:\s*'{layer}'.*?\}},)", re.DOTALL)
        m = id_pat.search(new_block)
        if not m:
            continue
        entry = m.group(1)
        new_entry = entry
        for t in NEW_TERRITORIES:
            if layer not in present[t]:
                continue
            # Replace `t: 'unavailable'` → `t: 'available'` (only inside this entry)
            new_entry = re.sub(
                rf"\b{t}\s*:\s*'unavailable'",
                f"{t}: 'available'",
                new_entry,
            )
        if new_entry != entry:
            new_block = new_block.replace(entry, new_entry)
            flipped = sum(1 for t in NEW_TERRITORIES if layer in present[t])
            flips += flipped
            print(f"  flip {layer}: {flipped} territories → available")

    if new_block == block:
        return 0
    src = src.replace(block, new_block)
    if dry_run:
        return flips
    CONFIG_TS.write_text(src, encoding='utf-8', newline='\n')
    return flips


def bump_cache_busters(present: dict[str, list[str]], dry_run: bool) -> int:
    """Increment the per-parquet cache buster in config.ts for layers we
    just regenerated. The bump is shared across territories — incrementing it
    once invalidates the cached parquet for all territories."""
    src = CONFIG_TS.read_text(encoding='utf-8')
    original = src
    layers_to_bump = set()
    for layers in present.values():
        for layer in layers:
            # Bumpable parquets follow sat_<layer> naming
            if layer in ('flood_risk', 'territorial_scores', 'land_use'):
                # Different parquet names — handle case-by-case below
                pass
            layers_to_bump.add(layer)

    name_map = {
        'territorial_scores': 'overture_scores',
        'flood_risk': 'hex_flood_risk',
        'land_use': 'sat_land_use',
    }
    bumped = 0
    for layer in layers_to_bump:
        parquet_key = name_map.get(layer, f"sat_{layer}")
        pat = re.compile(rf"({parquet_key}:\s*['\"]\?v=)(\d+)(['\"])")
        m = pat.search(src)
        if not m:
            continue
        new_n = int(m.group(2)) + 1
        new_str = f"{m.group(1)}{new_n}{m.group(3)}"
        src = src.replace(m.group(0), new_str)
        bumped += 1
        print(f"  cb {parquet_key}: v{m.group(2)} → v{new_n}")

    if dry_run:
        return bumped
    if src != original:
        CONFIG_TS.write_text(src, encoding='utf-8', newline='\n')
    return bumped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    dry = args.dry_run

    present = detect_present_pairs()
    print("Bundled summaries present per territory:")
    for t, layers in present.items():
        print(f"  {t}: {layers or '(none)'}")
    print()

    if not any(present.values()):
        print("Nothing to apply.")
        return 0

    print("Patching deptSummaries.ts...")
    added = patch_dept_summaries(present, dry)
    print()
    print("Patching coverage in config.ts...")
    flipped = patch_coverage(present, dry)
    print()
    print("Bumping cache busters in config.ts...")
    bumped = bump_cache_busters(present, dry)

    print()
    print(f"Summary{' (dry-run)' if dry else ''}: +{added} imports, {flipped} coverage flips, {bumped} cache bumps")
    return 0


if __name__ == '__main__':
    sys.exit(main())
