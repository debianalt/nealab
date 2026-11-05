"""
People estimated per res-9 cell, for the click selection by radio (HexComparison).

Each building carries a dasymetric population (classify_and_allocate.py → est_personas, summed per
cell × radio in the crosswalk). The rule the author set (4-nov-2026): the estimated people of a
radio's buildings add up to the radio's census population. That holds exactly wherever the building
source has buildings; 221 radios have none (Corrientes 199, Chaco 15 in General Güemes, Formosa 7
in Ramón Lista: est_personas = 0, the crosswalk falls back to area weights). There the radio's
population is spread by those area weights and the cell is flagged `por_area`.

The script stops without writing if any radio's people differ from its census population by more
than half a person.

Output per territory: pipeline/output/[<prefix>]cell_population_2022.parquet
  h3index      res-9 cell
  n_edificios  buildings in the cell (all its radios)
  personas     estimated people in the cell (all its radios), float
  por_area     part of the cell's people came from the area fallback (radio without buildings)

Usage:
  python pipeline/build_cell_population.py [--territory corrientes]
Publish (then purge the CDN). upload_to_r2.upload_file already prepends the bucket: its key is
'data/…', not 'neahub/data/…' (that lands in neahub/neahub/data and the CDN answers 404):
  rclone copyto pipeline/output/<prefix>cell_population_2022.parquet r2:neahub/data/<prefix>cell_population_2022.parquet
  python scripts/cdn_cache.py purge data/<prefix>cell_population_2022.parquet
"""
import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from build_census_rates import TERRITORIES, crosswalk_path  # noqa: E402
from config import OUTPUT_DIR, get_territory  # noqa: E402

OUT_NAME = 'cell_population_2022.parquet'
RADIOS_NAME = 'census_radios_2022.parquet'
TOL = 0.5   # people


def out_dir(t: str) -> str:
    prefix = get_territory(t)['output_prefix']
    return os.path.join(OUTPUT_DIR, prefix) if prefix else OUTPUT_DIR


def polyfill_rows(t: str, radios: list[str], pob: pd.Series) -> pd.DataFrame:
    import geopandas as gpd
    import h3
    p = (os.path.join(OUTPUT_DIR, 'radios_misiones.parquet') if t == 'misiones'
         else os.path.join(OUTPUT_DIR, t, f'radios_{t}.parquet'))
    g = gpd.read_parquet(p)[['redcode', 'geometry']]
    g['redcode'] = g.redcode.astype(str)
    g = g[g.redcode.isin(radios)].to_crs(4326)
    rows = []
    for rc, geom in zip(g.redcode, g.geometry):
        cells = set()
        for part in getattr(geom, 'geoms', [geom]):
            cells |= set(h3.geo_to_cells(part.__geo_interface__, 9))
        if not cells:
            c = geom.representative_point()
            cells = {h3.latlng_to_cell(c.y, c.x, 9)}
        for h in cells:
            rows.append({'h3index': h, 'redcode': rc, 'weight': 1 / len(cells), 'n_buildings': 0,
                         'est_personas': 0.0, 'personas': pob[rc] / len(cells), 'por_area': True})
    missing = set(radios) - set(g.redcode)
    if missing:
        sys.exit(f'{t}: radios del censo sin crosswalk ni polígono: {sorted(missing)[:10]}')
    print(f'  {t}: {len(radios)} radios fuera del crosswalk, repartidos en {len(rows)} celdas de su polígono')
    return pd.DataFrame(rows)


def build(t: str) -> pd.DataFrame:
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight', 'n_buildings', 'est_personas'])
    xw['redcode'] = xw.redcode.astype(str)
    pob = pd.read_parquet(os.path.join(out_dir(t), RADIOS_NAME), columns=['redcode', 'pob_total'])
    pob = pob.set_index(pob.redcode.astype(str)).pob_total.astype(float)

    nb_radio = xw.groupby('redcode').n_buildings.transform('sum')
    sin_edif = nb_radio == 0
    # Codes outside the census (e.g. 540070000: no radio, no population) carry 0 people anyway.
    con_pob = xw.redcode.map(pob).fillna(0) > 0
    w_sum = xw[sin_edif & con_pob].groupby('redcode').weight.sum()
    bad_w = w_sum[(w_sum - 1).abs() > 0.01]
    if len(bad_w):
        sys.exit(f'{t}: {len(bad_w)} radios sin edificios con Σ peso ≠ 1 (p. ej. {bad_w.head(3).to_dict()})')

    xw['personas'] = xw.est_personas.astype(float)
    xw.loc[sin_edif, 'personas'] = xw.loc[sin_edif, 'weight'] * xw.loc[sin_edif, 'redcode'].map(pob).fillna(0)
    xw['por_area'] = sin_edif

    # Census radios missing from the crosswalk altogether (Corrientes 180491105, Esquina): their
    # population goes evenly to the res-9 cells of their polygon (the centroid's cell if none).
    faltan = [r for r in pob.index if pob[r] > 0 and r not in set(xw.redcode)]
    if faltan:
        xw = pd.concat([xw, polyfill_rows(t, faltan, pob)], ignore_index=True)

    # The rule: per radio, Σ people = census population.
    s = xw.groupby('redcode').personas.sum()
    cmp_ = pd.DataFrame({'est': s, 'censo': pob}).fillna(0)
    off = cmp_[(cmp_.est - cmp_.censo).abs() > TOL]
    if len(off):
        sys.exit(f'{t}: {len(off)} radios se apartan del censo más de {TOL} persona:\n{off.head(10)}')

    cell = (xw.groupby('h3index')
            .agg(n_edificios=('n_buildings', 'sum'), personas=('personas', 'sum'), por_area=('por_area', 'any'))
            .reset_index())
    cell['n_edificios'] = cell.n_edificios.round().astype('int32')
    cell['personas'] = cell.personas.astype('float32')
    print(f'{t}: {len(cmp_):,} radios, todos = censo (tol. {TOL}); '
          f'{int(sin_edif.groupby(xw.redcode).any().sum())} radios por área; '
          f'{len(cell):,} celdas ({int(cell.por_area.sum()):,} por área); '
          f'personas {cell.personas.sum():,.0f} = censo {pob.sum():,.0f}')
    return cell


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', choices=list(TERRITORIES))
    a = ap.parse_args()
    for t in TERRITORIES:
        if a.territory and t != a.territory:
            continue
        cell = build(t)
        path = os.path.join(out_dir(t), OUT_NAME)
        cell.to_parquet(path, index=False, compression='zstd')
        print(f'  → {path} ({os.path.getsize(path) / 1e6:.2f} MB)')


if __name__ == '__main__':
    main()
