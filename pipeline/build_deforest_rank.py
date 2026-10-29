"""
Department ranking for the deforestation layer: three readings of the same
Hansen loss, switchable in the panel.

  loss_ha_yr        hectares of tree cover lost per year, 2015–2024     ← default
  loss_pct_forest   % of the 2000 tree cover lost per year, 2015–2024
  loss_change_pct   change vs 2001–2010 (both periods 10 years); only where the
                    baseline loss is ≥ MIN_BASE_HA ha/yr — from 2 to 6 ha reads
                    +200 % and would top the list on noise

Per hexagon the layer carries c_loss_rate (% of the cell lost per year in
2015–2024, Hansen 30 m pixels inside the cell), c_loss_rate_baseline (same,
2001–2010) and c_treecover (% tree cover in 2000). Hexes are equal-area at H3
res 9, so a department's hectares are sums × the mean cell area. Hansen tree
cover counts plantations: in Misiones part of the "loss" is pine and eucalyptus
harvest, which the panel note says.

Reads the dept parquets the CDN serves and writes a `rank` block into each
department of the deforestation dept summary JSON (src/lib/data). Re-run after
a Hansen refresh:

    python pipeline/build_deforest_rank.py            # all territories
"""

import json
import os
import sys

import h3
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import TERRITORY_CONFIGS, get_territory

CDN = 'https://cdn.spatia.ar/data'
SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
HEX_HA = h3.average_hexagon_area(9, unit='km^2') * 100
MIN_BASE_HA = 50   # ha/yr of 2001–2010 loss below which the change is not shown


def summary_path(tid: str) -> str:
    name = ('sat_deforestation_dynamics_dept_summary.json' if tid == 'misiones'
            else f'{tid}_sat_deforestation_dynamics_summary.json')
    return os.path.join(SRC_DATA_DIR, name)


def main() -> None:
    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    for tid in TERRITORY_CONFIGS:
        path = summary_path(tid)
        if not os.path.exists(path):
            print(f'{tid}: no summary, skipped')
            continue
        with open(path, encoding='utf-8') as f:
            summary = json.load(f)
        prefix = get_territory(tid)['output_prefix']
        urls = {f"{CDN}/{prefix}sat_dpto/sat_deforestation_dynamics_{d['parquetKey']}.parquet": d['parquetKey']
                for d in summary['departments']}
        url_list = ', '.join(f"'{u}'" for u in urls)
        agg = con.execute(f"""
            SELECT filename,
                   SUM(COALESCE(c_loss_rate, 0)) / 100 AS loss,
                   SUM(COALESCE(c_loss_rate_baseline, 0)) / 100 AS base,
                   SUM(COALESCE(c_treecover, 0)) / 100 AS forest
            FROM read_parquet([{url_list}], filename = true)
            GROUP BY filename""").df()
        agg['parquetKey'] = agg.filename.map(urls)
        by = agg.set_index('parquetKey')
        for d in summary['departments']:
            k = d['parquetKey']
            if k not in by.index:
                d['rank'] = None
                continue
            r = by.loc[k]
            loss_ha, base_ha, forest_ha = r.loss * HEX_HA, r.base * HEX_HA, r.forest * HEX_HA
            d['rank'] = {
                'loss_ha_yr': round(float(loss_ha)),
                'loss_pct_forest': round(float(100 * loss_ha / forest_ha), 2) if forest_ha >= 1 else None,
                'loss_change_pct': round(float(100 * (loss_ha - base_ha) / base_ha)) if base_ha >= MIN_BASE_HA else None,
                'loss_base_ha_yr': round(float(base_ha)),
            }
        summary.setdefault('metadata', {})['rank'] = {
            'metric': 'loss_ha_yr',
            'definition': 'Pérdida de cobertura arbórea Hansen GFC (30 m): ha/año 2015–2024, % de la cobertura '
                          '2000 por año, y cambio respecto de 2001–2010 (sólo con base ≥ '
                          f'{MIN_BASE_HA} ha/año)',
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        top = sorted((d for d in summary['departments'] if d.get('rank')), key=lambda d: -d['rank']['loss_ha_yr'])[:3]
        print(f"{tid}: {len(summary['departments'])} depts · top " +
              ', '.join(f"{d.get('dpto') or d.get('distrito') or d.get('municipio')} {d['rank']['loss_ha_yr']} ha/año" for d in top),
              flush=True)


if __name__ == '__main__':
    main()
