"""Hexoma A1 — esquema `suelo`: suelo y relieve por celda H3 en unidades físicas, NEA AR.

Lee lo que bajó pipeline/gee_export_suelo_relieve.py (%HEXOMA_FUENTES%/suelo/) y arma:

    suelo.fuentes   asset, resolución, rótulo y archivos (bytes, sha256)
    suelo.h3        una fila por celda:
                      SoilGrids 2.0, 0–30 cm ponderado por espesor, valor del píxel de 250 m que
                      contiene el centro de la celda (sin interpolar). Es un MODELO (predicción con
                      aprendizaje automático), no una medición; NULL = sin predicción (agua, ciudad).
                      FABDEM: media de los píxeles de 3" (~90 m) cuyo centro cae dentro de la celda
                      (n_px_relieve, ~13), con la pendiente calculada a 30 m.

Unidades (conversión de las enteras de SoilGrids, ver https://www.isric.org/explore/soilgrids/faq-soilgrids):
arcilla, arena, limo g/kg ÷ 10 → %; COS dg/kg ÷ 10 → g/kg; pH ×10 ÷ 10; CIC mmol(c)/kg ÷ 10 →
cmol(+)/kg; densidad aparente cg/cm³ ÷ 100 → g/cm³; fragmentos gruesos cm³/dm³ ÷ 10 → % vol.;
nitrógeno cg/kg ÷ 100 → g/kg.

Idempotente. Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_suelo.py
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from pyproj import Transformer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
SUELO = FUENTES_DIR / 'suelo'
CENTROS = FUENTES_DIR / 'clima' / 'centros.parquet'

ROT_SG = 'SoilGrids 2.0 (ISRIC), 250 m, 0–30 cm ponderado 5/10/15 cm: modelo, no medición'
ROT_FAB = 'FABDEM v1.2 (Copernicus GLO-30 sin dosel ni edificios), pendiente a 30 m, media a 3"'

# var SoilGrids → (columna, divisor, descripción)
SG = {
    'clay': ('arcilla_pct', 10, 'arcilla, % en masa'),
    'sand': ('arena_pct', 10, 'arena, % en masa'),
    'silt': ('limo_pct', 10, 'limo, % en masa'),
    'soc': ('cos_g_kg', 10, 'carbono orgánico del suelo, g/kg'),
    'phh2o': ('ph_h2o', 10, 'pH en agua'),
    'cec': ('cic_cmol_kg', 10, 'capacidad de intercambio catiónico, cmol(+)/kg'),
    'bdod': ('dap_g_cm3', 100, 'densidad aparente de la fracción fina, g/cm³'),
    'cfvo': ('fragmentos_pct', 10, 'fragmentos gruesos, % en volumen'),
    'nitrogen': ('n_g_kg', 100, 'nitrógeno total, g/kg'),
}
REL = {
    'pendiente_media_grados': 'pendiente media, grados',
    'elev_media_m': 'elevación media, m',
    'elev_min_m': 'elevación mínima, m',
    'elev_max_m': 'elevación máxima, m',
    'n_px_relieve': 'píxeles de 3" con centro dentro de la celda',
}


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def soilgrids(cen: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({'h3': cen.h3.values})
    tr = None
    for var, (col, div, _) in SG.items():
        z = np.load(SUELO / f'soilgrids_{var}_0-30cm.npz')
        x0, sx, y0, sy, w, h = (float(v) for v in z['grilla'])
        if tr is None:
            tr = Transformer.from_crs('EPSG:4326', str(z['wkt']), always_xy=True)
            x, y = tr.transform(cen.lng.values, cen.lat.values)
            cc = np.floor((x - x0) / sx).astype(int)
            ff = np.floor((y - y0) / sy).astype(int)
            assert (cc >= 0).all() and (cc < w).all() and (ff >= 0).all() and (ff < h).all()
        v = z['valor'][ff, cc].astype(float)
        v[v < 0] = np.nan
        out[col] = v / div
    log(f'SoilGrids: {len(out):,} celdas; sin predicción: {out.arcilla_pct.isna().sum():,}')
    return out


def relieve(cen: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute('LOAD h3')
    partes = []
    for f in sorted((SUELO / 'fabdem').glob('fabdem_*.npz')):
        z = np.load(f)
        x0, dx, y0, dy, w, h = (float(v) for v in z['grilla'])
        ff, cc = np.mgrid[0:int(h), 0:int(w)]
        df = pd.DataFrame({
            'lng': (x0 + (cc.ravel() + 0.5) * dx), 'lat': (y0 + (ff.ravel() + 0.5) * dy),
            's': z['slope_x100'].ravel().astype(np.int32), 'e': z['elev_media'].ravel().astype(np.int32),
            'emin': z['elev_min'].ravel(), 'emax': z['elev_max'].ravel()})
        con.register('px', df)
        partes.append(con.execute("""
            SELECT h3_h3_to_string(h3_latlng_to_cell(lat, lng, 9)) AS h3, sum(s) AS ss, sum(e) AS se,
                   min(emin) AS emin, max(emax) AS emax, count(*) AS n
            FROM px GROUP BY 1""").df())
        con.unregister('px')
    r = pd.concat(partes).groupby('h3').agg(ss=('ss', 'sum'), se=('se', 'sum'), emin=('emin', 'min'),
                                             emax=('emax', 'max'), n=('n', 'sum')).reset_index()
    r['pendiente_media_grados'] = r.ss / r.n / 100
    r['elev_media_m'] = r.se / r.n
    r = r.rename(columns={'emin': 'elev_min_m', 'emax': 'elev_max_m', 'n': 'n_px_relieve'})
    r = cen[['h3']].merge(r[['h3'] + list(REL)], on='h3', how='left')
    log(f'FABDEM: {r.n_px_relieve.notna().sum():,} celdas con relieve; mediana {r.n_px_relieve.median():.0f} píxeles por celda')
    return r


def main() -> None:
    t0 = time.time()
    cen = pd.read_parquet(CENTROS)
    df = soilgrids(cen).merge(relieve(cen), on='h3', validate='1:1')
    for c in ('elev_min_m', 'elev_max_m', 'n_px_relieve'):
        df[c] = df[c].astype('Int32')
    archivos = [{'archivo': str(f.relative_to(SUELO)), 'bytes': f.stat().st_size,
                 'sha256': hashlib.sha256(f.read_bytes()).hexdigest()}
                for f in sorted(SUELO.rglob('*.npz'))]
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS suelo CASCADE')
        cur.execute('CREATE SCHEMA suelo')
        cur.execute("COMMENT ON SCHEMA suelo IS 'Hexoma A1: suelo (SoilGrids 2.0) y relieve (FABDEM) por celda en "
                    "unidades físicas, NEA AR. pipeline/hexoma/build_hexoma_suelo.py'")
        cur.execute('CREATE TABLE suelo.fuentes (fuente text PRIMARY KEY, asset text, resolucion text, rotulo text, '
                    'archivos jsonb, cargado timestamptz DEFAULT now())')
        cur.execute('INSERT INTO suelo.fuentes VALUES (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s)',
                    ('soilgrids', 'projects/soilgrids-isric/<var>_mean', '250 m', ROT_SG,
                     json.dumps([a for a in archivos if 'soilgrids' in a['archivo']]),
                     'fabdem', 'projects/sat-io/open-datasets/FABDEM', '30 m → 3"', ROT_FAB,
                     json.dumps([a for a in archivos if 'fabdem' in a['archivo']])))
        cols = ([f'{c} real' for c, _, _ in SG.values()]
                + ['pendiente_media_grados real', 'elev_media_m real', 'elev_min_m int', 'elev_max_m int',
                   'n_px_relieve int'])
        cur.execute(f'CREATE TEMP TABLE t (h3 text, {", ".join(cols)})')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='')
        buf.seek(0)
        cur.copy_expert(f"COPY t ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        cur.execute('CREATE TABLE suelo.h3 AS SELECT h3::h3index AS h3, ' +
                    ', '.join(c for c in df.columns if c != 'h3') + ' FROM t')
        cur.execute('ALTER TABLE suelo.h3 ADD PRIMARY KEY (h3)')
        for c, _, d in SG.values():
            cur.execute(f'COMMENT ON COLUMN suelo.h3.{c} IS {q(d + ". " + ROT_SG)}')
        for c, d in REL.items():
            cur.execute(f'COMMENT ON COLUMN suelo.h3.{c} IS {q(d + ". " + ROT_FAB)}')
        cur.execute("COMMENT ON TABLE suelo.h3 IS 'Suelo y relieve por celda. Suelo: píxel de 250 m que contiene el "
                    "centro. Relieve: media de los píxeles de 3\" con centro en la celda. Fotos, sin serie temporal'")
        cur.execute('ANALYZE suelo.h3')
    verificar(conn)
    log(f'listo en {time.time() - t0:.0f} s')


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""SELECT t.territorio, count(*), round(avg(arcilla_pct)::numeric, 1), round(avg(arena_pct)::numeric, 1),
                              round(avg(ph_h2o)::numeric, 2), round(avg(cos_g_kg)::numeric, 1),
                              round(avg(elev_media_m)::numeric, 0), round(avg(pendiente_media_grados)::numeric, 2),
                              count(*) FILTER (WHERE arcilla_pct IS NULL), count(*) FILTER (WHERE n_px_relieve IS NULL)
                       FROM suelo.h3 s JOIN hexagonos x USING (h3) JOIN territorios t USING (territorio_id)
                       GROUP BY 1 ORDER BY 1""")
        print('  territorio, celdas, arcilla %, arena %, pH, COS g/kg, elevación m, pendiente °, sin suelo, sin relieve')
        for r in cur.fetchall():
            print('  ', r)
        for nombre, lat, lng in (('Bernardo de Irigoyen', -26.25, -53.65), ('Posadas', -27.386, -55.971),
                                 ('Ituzaingó (arenales)', -27.59, -56.68)):
            cur.execute(f"""SELECT arcilla_pct, arena_pct, ph_h2o, elev_media_m, pendiente_media_grados FROM suelo.h3
                            WHERE h3 = h3_lat_lng_to_cell(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 9)""")
            print(f'  {nombre}: arcilla, arena, pH, elevación, pendiente =', cur.fetchone())
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('suelo.h3'))")
        print(f'  tamaño suelo.h3: {cur.fetchone()[0]}')


if __name__ == '__main__':
    main()
