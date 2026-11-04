"""
Trayectorias de actividad económica: valores del año base por hexágono para el gráfico de transiciones.

  pct_universitario_2010  universitario completo, % de 18+, Censo 2010 (censo_2010.poblacion_2010: p18a_unive / p_18a,
                          la misma tabla y definición que 2022).
  viirs_2014[_neto], viirs_2025_neto
                          radiancia nocturna media 2014 (NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG, media de los 12 meses;
                          el producto empieza en 2014-01), el mismo producto y muestreo que viirs_mean_radiance (2025).

Mismo método que el año actual, para que el cambio no salga del método:
- 2010 se reparte con el mismo criterio dasimétrico que 2022 (build_census_rates.py): peso = edificios del hexágono /
  edificios del radio. Los edificios por hexágono son los del crosswalk 2022 (n_buildings); cada hexágono va al radio
  2010 que contiene su centro. Tasa = Σ w·numerador / Σ w·denominador, radios con base < MIN_BASE fuera.
- 2014 se muestrea bilineal en el centro de cada hexágono (compute_viirs_buildings_h3.sample_raster_bilinear), sin dato → 0.
Controles: la tasa provincial de los hexágonos 2010 tiene que dar la de los radios (±0,5 pts) y se informa la parte de
la población 18+ de 2010 que quedó en radios sin hexágono con edificios.

Insumos: output/<t>/sat_economic_activity.parquet (lo que sirve la CDN; build_census_rates.fetch), el crosswalk
dasimétrico, viirs_<t>_2014.tif (gee_export_viirs.py --year 2014 --gcs). Publicar con:

    python pipeline/build_econ_trayectorias.py
    python pipeline/refresh_dept_parquets.py --layer economic_activity --territory <t> --schema-change --backup-dir C:/Users/ant/r2-econ-backup-2026-11
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
    g = gpd.read_postgis(f"""SELECT redcode::text AS redcode, p_18a, p18a_unive, geometry
                             FROM censo_2010.poblacion_2010 WHERE redcode::text LIKE '{codprov}%%'""",
                         conn, geom_col='geometry')
    if g.crs is None:
        g = g.set_crs(4326)
    return g.to_crs(4326)


def cell_poly(h: str) -> Polygon:
    return Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(h)])


def univ_2010(t: str, codprov: str, hexes: pd.Series) -> pd.Series:
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
    m = j.merge(r[['redcode', 'p_18a', 'p18a_unive']], on='redcode')
    m = m[m.p_18a >= MIN_BASE]
    g = m.assign(n=m.w * m.p18a_unive, d=m.w * m.p_18a).groupby('h3index')[['n', 'd']].sum()
    rate = (g.n / g.d * 100).round(1)
    # controles
    ok = r[r.p_18a >= MIN_BASE]
    prov = ok.p18a_unive.sum() / ok.p_18a.sum() * 100
    hexp = (rate * g.d).sum() / g.d.sum()
    lost = 1 - ok[ok.redcode.isin(j.redcode)].p_18a.sum() / ok.p_18a.sum()
    print(f'{t} univ 2010: {len(rate):,} hexágonos, provincia {prov:.1f} %, hexágonos {hexp:.1f} %, '
          f'población 18+ en radios sin hexágono {100 * lost:.1f} %')
    if abs(prov - hexp) > 0.5:
        sys.exit(f'{t}: los hexágonos 2010 no dan la provincia ({hexp:.1f} vs {prov:.1f})')
    return hexes.map(rate)


def univ_cambio_depto(t: str, codprov: str) -> None:
    """rank.univ_cambio_pts: universitario completo, % de 18+, 2022 − 2010 por departamento (conteos de radios, misma
    base 18+ en los dos censos; el ranking de la pestaña va sobre 25+, que 2010 no tiene por radio)."""
    import json
    from build_univ25_rank import norm
    cur = psycopg2.connect(PG).cursor()
    cur.execute("SELECT dpto, SUM(p18a_unive), SUM(p_18a) FROM censo_2010.poblacion_2010 WHERE redcode::text LIKE %s AND dpto IS NOT NULL GROUP BY 1",
                (codprov + '%',))
    r10 = {norm(d): 100 * float(u) / float(p) for d, u, p in cur.fetchall()}
    cur.execute("""SELECT g.dpto, SUM(n.p18a_unive), SUM(n.p_18a) FROM censo_2022.censo_niveducativo_2022 n
                   JOIN censo_2022.radios_geom g USING (redcode) WHERE n.redcode::text LIKE %s AND g.dpto IS NOT NULL GROUP BY 1""", (codprov + '%',))
    r22 = {norm(d): 100 * float(u) / float(p) for d, u, p in cur.fetchall()}
    fname = 'sat_economic_activity_dept_summary.json' if t == 'misiones' else f'{t}_sat_economic_activity_summary.json'
    path = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data', fname)
    with open(path, encoding='utf-8') as f:
        summary = json.load(f)
    missing = [d['dpto'] for d in summary['departments'] if norm(d['dpto']) not in r10 or norm(d['dpto']) not in r22]
    if missing:
        sys.exit(f'{t}: sin 2010 o 2022 para {missing}')
    for d in summary['departments']:
        k = norm(d['dpto'])
        d['rank'] = {**(d.get('rank') or {}), 'univ_cambio_pts': round(r22[k] - r10[k], 1)}
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    ch = sorted(((d['rank']['univ_cambio_pts'], d['dpto']) for d in summary['departments']))
    print(f'  univ 18+ 2010→2022 por departamento: de {ch[0][0]:+.1f} ({ch[0][1]}) a {ch[-1][0]:+.1f} ({ch[-1][1]}) pts')


def main() -> None:
    for t, codprov in TERRITORIES.items():
        prefix = get_territory(t)['output_prefix']
        out = os.path.join(t_dir(t), 'sat_economic_activity.parquet')
        fetch(f'{CDN}/{prefix}sat_economic_activity.parquet', out)
        df = pd.read_parquet(out)
        df['pct_universitario_2010'] = univ_2010(t, codprov, df.h3index)

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

        both = df.pct_universitario.notna() & df.pct_universitario_2010.notna()
        print(f'{t}: univ 2010 → 2022 en {both.sum():,} hexágonos (de {df.pct_universitario.notna().sum():,} con 2022), '
              f'mediana {df.loc[both, "pct_universitario_2010"].median():.1f} → {df.loc[both, "pct_universitario"].median():.1f} %; '
              f'luz neta media {df.viirs_2014_neto.mean():.3f} → {df.viirs_2025_neto.mean():.3f}, '
              f'hexágonos con luz neta ≥ 1: {(df.viirs_2014_neto >= 1).mean():.1%} → {(df.viirs_2025_neto >= 1).mean():.1%}')
        df.to_parquet(out, index=False, compression='zstd')
        univ_cambio_depto(t, codprov)
        print(f'  escrito {out}')


if __name__ == '__main__':
    main()
