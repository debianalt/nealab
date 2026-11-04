"""
Trayectorias de las capas censales: valores del año base por hexágono para el gráfico de transiciones y el cambio
por departamento al lado del ranking.

  economic_activity  pct_universitario_2010 (Censo 2010); viirs_2014[_neto], viirs_2025_neto (VIIRS 2014 y 2025).
  education_capital  c_no_schooling_2010, c_secondary_plus_2010, c_higher_edu_2010, c_university_2010 (Censo 2010).

Censo 2010: censo_2010.poblacion_2010, la misma tabla y categorías que censo_2022.censo_niveducativo_2022 (p_18a,
p18a_sin_i, p18a_secun, p18a_terci, p18a_unive; en Misiones sin instrucción + primario completo o más suman 95,8 % en
2010 y 95,4 % en 2022). Se reparte con el mismo criterio dasimétrico que 2022 (build_census_rates.py): peso = edificios
del hexágono / edificios del radio, con los edificios por hexágono del crosswalk 2022 y cada hexágono partido entre los
radios 2010 que toca por área. Tasa = Σ w·numerador / Σ w·denominador, radios con base < MIN_BASE fuera. Control: la
tasa provincial de los hexágonos tiene que dar la de los radios (±0,5 pts).
VIIRS: VCMSLCFG (el producto de 2025; empieza en 2014), muestreo bilineal en el centro del hexágono; luz neta =
radiancia − percentil 10 de la provincia en cada año (el fondo pasó de 0,06 a 0,34 nW/cm²/sr).
Cambio por departamento (rank.*_cambio_pts): % de 18+ en 2022 − en 2010, con los conteos de los radios.

    python pipeline/build_census_trayectorias.py --layer education_capital
    python pipeline/refresh_dept_parquets.py --layer education_capital --territory <t> --schema-change --backup-dir <carpeta NUEVA>
"""
import os
import sys

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import psycopg2
from shapely.geometry import Polygon

sys.path.insert(0, os.path.dirname(__file__))
from build_census_rates import MIN_BASE, TERRITORIES, crosswalk_path   # noqa: E402
from compute_viirs_buildings_h3 import sample_raster_bilinear           # noqa: E402
from config import OUTPUT_DIR, get_territory                            # noqa: E402
from refresh_dept_parquets import CDN, fetch                            # noqa: E402

PG = 'dbname=posadas user=postgres'
BACKGROUND_Q = 0.10


def t_dir(t: str) -> str:
    prefix = get_territory(t)['output_prefix']
    return os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR


def radios_2010(codprov: str) -> gpd.GeoDataFrame:
    conn = psycopg2.connect(PG)
    g = gpd.read_postgis(f"""SELECT redcode::text AS redcode, p_18a, p18a_sin_i, p18a_secun, p18a_terci, p18a_unive, geometry
                             FROM censo_2010.poblacion_2010 WHERE redcode::text LIKE '{codprov}%%'""",
                         conn, geom_col='geometry')
    if g.crs is None:
        g = g.set_crs(4326)
    return g.to_crs(4326)


def cell_poly(h: str) -> Polygon:
    return Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(h)])


def rates_2010(t: str, codprov: str, hexes: pd.Series, spec: dict) -> dict:
    """spec: {columna_2010: [numeradores]}; denominador p_18a."""
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'n_buildings'])
    nb = xw.groupby('h3index').n_buildings.sum()
    nb = nb[nb > 0]
    cells = gpd.GeoDataFrame({'h3index': nb.index, 'nb': nb.values},
                             geometry=[cell_poly(h) for h in nb.index], crs=4326)
    r = radios_2010(codprov)
    # Cada hexágono se reparte entre los radios 2010 que toca, por área de la intersección: los edificios del
    # hexágono se suponen parejos dentro de él (un radio urbano chico puede no contener el centro de ningún hexágono).
    utm = cells.estimate_utm_crs()
    j = gpd.overlay(cells.to_crs(utm), r[['redcode', 'geometry']].to_crs(utm), how='intersection', keep_geom_type=True)
    j['frac'] = j.geometry.area / j.h3index.map(cells.to_crs(utm).set_index('h3index').geometry.area)
    j['b'] = j.nb * j.frac
    j['w'] = j.b / j.groupby('redcode').b.transform('sum')
    ok = r[r.p_18a >= MIN_BASE]
    lost = 1 - ok[ok.redcode.isin(j.redcode)].p_18a.sum() / ok.p_18a.sum()
    # Radios 2010 sin hexágono con edificios (Corrientes: 8 % de la población 18+): por área entre las celdas que
    # cubren, como el respaldo areal del crosswalk 2022 (build_dasymetric_crosswalk.py).
    fb = []
    for rc, geom in r[~r.redcode.isin(j.redcode)][['redcode', 'geometry']].itertuples(index=False):
        hs = set()
        for part in getattr(geom, 'geoms', [geom]):
            hs |= set(h3.geo_to_cells(part.__geo_interface__, 9))
        if not hs:
            c = geom.representative_point()
            hs = {h3.latlng_to_cell(c.y, c.x, 9)}
        fb += [(h, rc, 1 / len(hs)) for h in hs]
    xw = pd.concat([j[['h3index', 'redcode', 'w']], pd.DataFrame(fb, columns=['h3index', 'redcode', 'w'])], ignore_index=True)
    print(f'{t} 2010: población 18+ en radios sin hexágono con edificios {100 * lost:.1f} % (repartida por área)')
    m = xw.merge(pd.DataFrame(r.drop(columns='geometry')), on='redcode')
    m = m[m.p_18a >= MIN_BASE]
    out = {}
    for col, nums in spec.items():
        num = m[nums].sum(axis=1)
        g = pd.DataFrame({'h3index': m.h3index, 'n': m.w * num, 'd': m.w * m.p_18a}).groupby('h3index')[['n', 'd']].sum()
        rate = (g.n / g.d * 100).round(1)
        prov = ok[nums].sum(axis=1).sum() / ok.p_18a.sum() * 100
        hexp = (rate * g.d).sum() / g.d.sum()
        print(f'  {col}: {len(rate):,} hexágonos, provincia {prov:.1f} %, hexágonos {hexp:.1f} %')
        if abs(prov - hexp) > 0.5:
            sys.exit(f'{t}/{col}: los hexágonos 2010 no dan la provincia ({hexp:.1f} vs {prov:.1f})')
        out[col] = hexes.map(rate)
    return out


def cambio_depto(layer: str, t: str, codprov: str, spec: dict) -> None:
    """spec: {campo_rank: [numeradores]}: % de 18+ en 2022 − en 2010 por departamento (conteos de radios, misma base
    en los dos censos; los rankings sobre 25+ no tienen 2010 por radio)."""
    import json
    from build_univ25_rank import norm
    cols = sorted({c for nums in spec.values() for c in nums})
    cur = psycopg2.connect(PG).cursor()
    cur.execute(f"SELECT dpto, SUM(p_18a), {', '.join(f'SUM({c})' for c in cols)} FROM censo_2010.poblacion_2010 "
                "WHERE redcode::text LIKE %s AND dpto IS NOT NULL GROUP BY 1", (codprov + '%',))
    r10 = {norm(row[0]): dict(zip(['p_18a', *cols], map(float, row[1:]))) for row in cur.fetchall()}
    cur.execute(f"SELECT g.dpto, SUM(n.p_18a), {', '.join(f'SUM(n.{c})' for c in cols)} "
                "FROM censo_2022.censo_niveducativo_2022 n JOIN censo_2022.radios_geom g USING (redcode) "
                "WHERE n.redcode::text LIKE %s AND g.dpto IS NOT NULL GROUP BY 1", (codprov + '%',))
    r22 = {norm(row[0]): dict(zip(['p_18a', *cols], map(float, row[1:]))) for row in cur.fetchall()}
    fname = f'sat_{layer}_dept_summary.json' if t == 'misiones' else f'{t}_sat_{layer}_summary.json'
    path = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data', fname)
    with open(path, encoding='utf-8') as f:
        summary = json.load(f)
    missing = [d['dpto'] for d in summary['departments'] if norm(d['dpto']) not in r10 or norm(d['dpto']) not in r22]
    if missing:
        sys.exit(f'{t}: sin 2010 o 2022 para {missing}')

    def pct(r, nums):
        return 100 * sum(r[c] for c in nums) / r['p_18a']
    for d in summary['departments']:
        k = norm(d['dpto'])
        d['rank'] = {**(d.get('rank') or {}),
                     **{field: round(pct(r22[k], nums) - pct(r10[k], nums), 1) for field, nums in spec.items()}}
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    for field in spec:
        ch = sorted((d['rank'][field], d['dpto']) for d in summary['departments'])
        print(f'  {field}: de {ch[0][0]:+.1f} ({ch[0][1]}) a {ch[-1][0]:+.1f} ({ch[-1][1]}) pts')


LAYERS = {
    'economic_activity': {
        'rates': {'pct_universitario_2010': ['p18a_unive']},
        'cambio': {'univ_cambio_pts': ['p18a_unive']},
        'viirs': True,
    },
    'education_capital': {
        'rates': {'c_no_schooling_2010': ['p18a_sin_i'], 'c_secondary_plus_2010': ['p18a_secun'],
                  'c_higher_edu_2010': ['p18a_terci', 'p18a_unive'], 'c_university_2010': ['p18a_unive']},
        'cambio': {'noschool_cambio_pts': ['p18a_sin_i'], 'univ_cambio_pts': ['p18a_unive']},
        'viirs': False,
    },
}
# Columna 2022 de cada columna 2010 (economic_activity no sigue el patrón c_*_2010).
CURRENT = {'pct_universitario_2010': 'pct_universitario'}


def add_viirs(t: str, df: pd.DataFrame) -> None:
    tif = os.path.join(t_dir(t), f'viirs_{t}_2014.tif')
    if not os.path.exists(tif):
        sys.exit(f'falta {tif} (gee_export_viirs.py --territory {t} --year 2014 --gcs)')
    ll = np.array([h3.cell_to_latlng(h) for h in df.h3index])
    v = sample_raster_bilinear(tif, ll[:, 1], ll[:, 0])
    df['viirs_2014'] = np.round(np.nan_to_num(v, nan=0.0), 4)
    # El fondo del producto mensual cambia entre años (Misiones, percentil 10: 0,06 en 2014, 0,34 en 2025): sin
    # restarlo, media provincia «se ilumina». Luz neta = radiancia − fondo del año en la provincia, mínimo 0; sólo
    # para comparar años (el mapa sigue con viirs_mean_radiance).
    for col, out_col in (('viirs_2014', 'viirs_2014_neto'), ('viirs_mean_radiance', 'viirs_2025_neto')):
        fondo = df[col].quantile(BACKGROUND_Q)
        df[out_col] = (df[col] - fondo).clip(lower=0).round(4)
        print(f'  {col}: fondo {fondo:.3f} nW/cm²/sr')


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--layer', choices=list(LAYERS), required=True)
    layer = ap.parse_args().layer
    cfg = LAYERS[layer]
    for t, codprov in TERRITORIES.items():
        prefix = get_territory(t)['output_prefix']
        out = os.path.join(t_dir(t), f'sat_{layer}.parquet')
        fetch(f'{CDN}/{prefix}sat_{layer}.parquet', out)
        df = pd.read_parquet(out)
        for col, s10 in rates_2010(t, codprov, df.h3index, cfg['rates']).items():
            df[col] = s10
            cur = CURRENT.get(col, col.removesuffix('_2010'))
            both = df[cur].notna() & df[col].notna()
            print(f'  {cur}: 2010 → 2022 en {both.sum():,} hexágonos (de {df[cur].notna().sum():,}), mediana '
                  f'{df.loc[both, col].median():.1f} → {df.loc[both, cur].median():.1f} %')
        if cfg['viirs']:
            add_viirs(t, df)
        df.to_parquet(out, index=False, compression='zstd')
        cambio_depto(layer, t, codprov, cfg['cambio'])
        print(f'  escrito {out}')


if __name__ == '__main__':
    main()
