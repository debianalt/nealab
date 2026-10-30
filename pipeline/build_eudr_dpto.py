"""
EUDR layer split per department, like the other hexagon layers, with a ranking.

The main-map EUDR layer used to read one cross-border res-7 parquet and filter it
in the browser by province: a full scan per territory, slow, and it never showed
Chaco, Formosa or Paraguay. This writes, for each of the 24 territories:

  {prefix}sat_dpto/sat_eudr_{parquetKey}.parquet   res-9 hexes of one department
  {prefix}sat_eudr.parquet                         the whole territory (provincial
                                                   averages for petal and lasso)
  src/lib/data/{tid}_sat_eudr_summary.json         department list + `rank` block
                                                   (Misiones: sat_eudr_dept_summary.json)

Source: data/eudr/hires/eudr_res9_combined.parquet, the res-9 grid /eudr/check
already queries, so the map and the check show the same figures. Department of
each hexagon: the deforestation layer's dept parquets the CDN serves (areal
crosswalk), with its department names, keys and centroids — no point-in-polygon.

Ranking, three readings of the same post-2020 Hansen loss (columns are fractions
of the cell × 100; res-9 cells are equal-area, so hectares are sums × cell area):

  loss_ha           ha of tree cover lost 2021–2025                     ← default
  loss_pct_forest   those ha as % of the 2000 tree cover (Hansen treecover2000)
  fire_native_ha    ha burned on native woody vegetation after 2020 (MODIS × MapBiomas)

The EUDR grid (pipeline/config_eudr.py) stops short of the northern Paraguayan
Chaco: a department whose hexes are < MIN_COVERAGE in the grid keeps its partial
hexes but gets no ranking bar (rank = null, `coverage` says how much there is),
and a territory with no department over it is not written (Alto Paraguay).

    curl -o pipeline/output/eudr_res9_combined.parquet https://cdn.spatia.ar/data/eudr/hires/eudr_res9_combined.parquet
    python pipeline/build_eudr_dpto.py                  # all territories
    python pipeline/build_eudr_dpto.py --territory chaco
    rclone copy pipeline/output/eudr_dpto r2:neahub/data --transfers 16
"""

import argparse
import json
import os
import sys

import h3

sys.path.insert(0, os.path.dirname(__file__))
from config import TERRITORY_CONFIGS, get_territory

CDN = 'https://cdn.spatia.ar/data'
HERE = os.path.dirname(__file__)
SRC_DATA_DIR = os.path.join(HERE, '..', 'src', 'lib', 'data')
RES9 = os.path.join(HERE, 'output', 'eudr_res9_combined.parquet')
OUT = os.path.join(HERE, 'output', 'eudr_dpto')
HEX_HA = h3.average_hexagon_area(9, unit='km^2') * 100
MIN_COVERAGE = 0.90

COLS = ['loss_post_2020_pct', 'fire_native_post_2020_pct', 'fire_post_2020_pct',
        'forest_cover_2020', 'forest_cover_current']


def summary_path(layer: str, tid: str) -> str:
    name = (f'sat_{layer}_dept_summary.json' if tid == 'misiones'
            else f'{tid}_sat_{layer}_summary.json')
    return os.path.join(SRC_DATA_DIR, name)


def copy_parquet(con, sql: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{path.replace(os.sep, '/')}' (FORMAT parquet, COMPRESSION zstd)")


def build(con, tid: str) -> None:
    with open(summary_path('deforestation_dynamics', tid), encoding='utf-8') as f:
        deforest = json.load(f)
    prefix = get_territory(tid)['output_prefix']
    urls = {f"{CDN}/{prefix}sat_dpto/sat_deforestation_dynamics_{d['parquetKey']}.parquet": d['parquetKey']
            for d in deforest['departments']}
    url_list = ', '.join(f"'{u}'" for u in urls)
    con.execute('CREATE OR REPLACE TEMP TABLE keymap (filename VARCHAR, parquetKey VARCHAR)')
    con.executemany('INSERT INTO keymap VALUES (?, ?)', list(urls.items()))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dept AS
        SELECT DISTINCT p.h3index, k.parquetKey
        FROM read_parquet([{url_list}], filename = true) p JOIN keymap k USING (filename)""")
    cols = ', '.join(f'e.{c}' for c in COLS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE t AS
        SELECT d.parquetKey, e.h3index, {cols}
        FROM dept d JOIN eudr e ON e.h3index = d.h3index""")
    n_dept, n_hit = con.execute('SELECT (SELECT COUNT(*) FROM dept), (SELECT COUNT(*) FROM t)').fetchone()
    cov = n_hit / n_dept if n_dept else 0
    dept_n = dict(con.execute('SELECT parquetKey, COUNT(*) FROM dept GROUP BY 1').fetchall())
    hit_n = dict(con.execute('SELECT parquetKey, COUNT(*) FROM t GROUP BY 1').fetchall())
    dept_cov = {k: hit_n.get(k, 0) / n for k, n in dept_n.items()}
    if not any(c >= MIN_COVERAGE for c in dept_cov.values()):
        print(f'{tid}: coverage {cov:.1%} ({n_hit:,}/{n_dept:,}), no department >= {MIN_COVERAGE:.0%}, NOT written', flush=True)
        return

    out_cols = ', '.join(['h3index'] + COLS)
    copy_parquet(con, f'SELECT {out_cols} FROM t ORDER BY h3index',
                 os.path.join(OUT, prefix, 'sat_eudr.parquet'))
    for key in sorted({d['parquetKey'] for d in deforest['departments']}):
        copy_parquet(con, f"SELECT {out_cols} FROM t WHERE parquetKey = '{key}' ORDER BY h3index",
                     os.path.join(OUT, prefix, 'sat_dpto', f'sat_eudr_{key}.parquet'))

    agg = {r[0]: r[1:] for r in con.execute("""
        SELECT parquetKey, COUNT(*),
               AVG(loss_post_2020_pct),
               SUM(COALESCE(loss_post_2020_pct, 0)) / 100,
               SUM(COALESCE(forest_cover_2020, 0)) / 100,
               SUM(COALESCE(fire_native_post_2020_pct, 0)) / 100,
               COUNT(fire_native_post_2020_pct)
        FROM t GROUP BY parquetKey""").fetchall()}
    departments = []
    for d in deforest['departments']:
        out = {k: v for k, v in d.items() if k not in ('avg_score', 'hex_count', 'rank')}
        a = agg.get(d['parquetKey'])
        c = dept_cov.get(d['parquetKey'], 0)
        if a is None:
            out.update(avg_score=None, hex_count=0, rank=None, coverage=round(c, 3))
        elif c < MIN_COVERAGE:
            out.update(avg_score=None, hex_count=int(a[0]), rank=None, coverage=round(c, 3))
        else:
            n, mean_loss, loss, forest, fire, n_fire = a
            loss_ha, forest_ha = loss * HEX_HA, forest * HEX_HA
            out.update(
                avg_score=round(float(mean_loss), 2) if mean_loss is not None else None,
                hex_count=int(n),
                rank={
                    'loss_ha': round(float(loss_ha)),
                    'loss_pct_forest': round(float(100 * loss_ha / forest_ha), 2) if forest_ha >= 1 else None,
                    # MapBiomas AR only: PY/BR hexes carry no value → no data, not 0.
                    'fire_native_ha': round(float(fire * HEX_HA)) if n_fire >= MIN_COVERAGE * n else None,
                },
            )
        departments.append(out)
    departments.sort(key=lambda d: -(d['rank'] or {}).get('loss_ha', -1))

    total, mean = con.execute('SELECT COUNT(*), AVG(loss_post_2020_pct) FROM t').fetchone()
    summary = {
        'territory': tid,
        'province': {'total_hexes': int(total), 'avg_score': round(float(mean), 2), 'unassigned': 0},
        'departments': departments,
        'metadata': {
            'source': 'data/eudr/hires/eudr_res9_combined.parquet (H3 res-9)',
            'rank': {
                'metric': 'loss_ha',
                'definition': 'Pérdida de cobertura arbórea Hansen GFC posterior al 31-12-2020 '
                              '(2021–2025): ha, % de la cobertura arbórea 2000, y ha quemadas '
                              'sobre vegetación nativa leñosa (MODIS MCD64A1 × MapBiomas)',
            },
        },
    }
    with open(summary_path('eudr', tid), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    top = [d for d in departments if d['rank']][:3]
    name = lambda d: d.get('dpto') or d.get('distrito') or d.get('municipio')
    print(f"{tid}: coverage {cov:.1%} · {len(departments)} depts · {mean:.2f} % mean · top " +
          ', '.join(f"{name(d)} {d['rank']['loss_ha']:,} ha" for d in top), flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', help='one territory id (default: all)')
    args = ap.parse_args()

    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute(f"CREATE VIEW eudr AS SELECT * FROM '{RES9.replace(os.sep, '/')}'")
    for tid in ([args.territory] if args.territory else TERRITORY_CONFIGS):
        if not os.path.exists(summary_path('deforestation_dynamics', tid)):
            print(f'{tid}: no deforestation summary, skipped')
            continue
        build(con, tid)


if __name__ == '__main__':
    main()
