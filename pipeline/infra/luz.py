"""Luces nocturnas de Misiones (Black Marble diario, VIIRS mensual y anual) → datos para escenarios de electricidad.

Sólo DATOS (fase 4): este script baja, guarda y resume; no detecta cortes.

Fuentes (Google Earth Engine):
  NASA/VIIRS/002/VNP46A2         Black Marble diario (desde 19-ene-2012), 15" (~460 m): radiancia nocturna corregida
                                 por BRDF, luna y atmósfera (DNB_BRDF_Corrected_NTL, nW·cm⁻²·sr⁻¹), bandera de calidad
                                 obligatoria (0 alta persistente, 1 alta efímera, 2 mala, 255 sin dato) y máscara de
                                 nubes (QF_Cloud_Mask, bits 6–7: 00 despejado seguro, 01 probable despejado,
                                 10 probable nublado, 11 nublado seguro).
  NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG   compuesto mensual sin luz difusa (avg_rad, cf_cvg), desde 2014-01.
  NOAA/VIIRS/DNB/ANNUAL_V22 (y V21)    compuestos anuales (median_masked, average_masked, cf_cvg).

Pasos:
  --bajar     cada día de VNP46A2 en la grilla nativa de Misiones + margen (BBOX de drenaje.py), por computePixels en
              paralelo; uint16: rad×10 (65535 = sin dato), calidad (255 = sin dato), nubes (65535 = sin dato).
              Un npz por día en %HEXOMA_FUENTES%/infra/blackmarble/AAAA/. Reanudable: lo bajado no se vuelve a pedir.
  --mensual   VCMSLCFG mensual y los anuales, todos los píxeles, a Parquet.
  --procesar  píxeles iluminados (mediana anual > UMBRAL_LUZ en algún año) → infra.luz_pixel; su serie diaria →
              Parquet por año; agregado diario por asentamiento BAHRA → infra.luz_asentamiento_diaria; mensual por
              celda h3 → infra.luz_mensual_h3.

Uso (desde neahub/):
    python pipeline/infra/luz.py --bajar [--hasta-min 9]      # tandas; repetir hasta que diga «completo»
    python pipeline/infra/luz.py --mensual
    python pipeline/infra/luz.py --procesar
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402

DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'blackmarble'
PASO = 1 / 240                     # 15" (grilla nativa de VNP46A2)
COL_DIARIA = 'NASA/VIIRS/002/VNP46A2'
COL_MENSUAL = 'NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG'
COL_ANUAL = ['NOAA/VIIRS/DNB/ANNUAL_V21', 'NOAA/VIIRS/DNB/ANNUAL_V22']
UMBRAL_LUZ = 0.5                   # nW·cm⁻²·sr⁻¹, mediana anual: píxel «iluminado»
HILOS = 8


def grilla() -> dict:
    x0 = math.floor(drenaje.BBOX[0] / PASO) * PASO
    y0 = math.ceil(drenaje.BBOX[3] / PASO) * PASO
    ancho = int(round((drenaje.BBOX[2] - x0) / PASO))
    alto = int(round((y0 - drenaje.BBOX[1]) / PASO))
    return {'x0': x0, 'dx': PASO, 'y0': y0, 'dy': -PASO, 'ancho': ancho, 'alto': alto}


def imagen_dia(ee, idx: str):
    im = ee.Image(f'{COL_DIARIA}/{idx}')
    rad = im.select('DNB_BRDF_Corrected_NTL').multiply(10).round().clamp(0, 65534).unmask(65535).toUint16().rename('rad')
    qf = im.select('Mandatory_Quality_Flag').unmask(255).toUint16().rename('qf')
    nub = im.select('QF_Cloud_Mask').unmask(65535).toUint16().rename('nubes')
    return rad.addBands(qf).addBands(nub)


def bajar_dia(ee, idx: str, g: dict) -> str:
    from gee_export_clima_diario import bajar
    fecha = dt.datetime.strptime(idx, '%Y_%m_%d').date()
    dest = DIR / f'{fecha.year}' / f'vnp46a2_{fecha:%Y%m%d}.npz'
    if dest.exists():
        return 'ya'
    r = bajar(imagen_dia(ee, idx), g)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.part.npz')
    np.savez_compressed(tmp, rad=r['rad'], qf=r['qf'].astype(np.uint8), nubes=r['nubes'],
                        grilla=np.array([g['x0'], g['dx'], g['y0'], g['dy'], g['ancho'], g['alto']]))
    tmp.replace(dest)
    return 'nuevo'


def bajar_diario(hasta_min: float) -> int:
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    g = grilla()
    ids = ee.ImageCollection(COL_DIARIA).aggregate_array('system:index').getInfo()
    falta = [i for i in ids if not (DIR / i[:4] / f'vnp46a2_{i.replace("_", "")}.npz').exists()]
    print(f'Black Marble: {len(ids):,} días en GEE; faltan {len(falta):,}; grilla {g["ancho"]}×{g["alto"]}', flush=True)
    t0, hechos, errores = time.time(), 0, []
    with ThreadPoolExecutor(HILOS) as ex:
        futs = {}
        it = iter(falta)
        for _ in range(HILOS * 2):
            i = next(it, None)
            if i:
                futs[ex.submit(bajar_dia, ee, i, g)] = i
        while futs:
            for f in as_completed(list(futs)):
                i = futs.pop(f)
                try:
                    f.result()
                    hechos += 1
                except Exception as e:  # noqa: BLE001
                    errores.append((i, str(e)[:120]))
                if hechos % 100 == 0:
                    print(f'  {hechos:,} días ({time.time() - t0:.0f} s)', flush=True)
                if time.time() - t0 < hasta_min * 60:
                    nxt = next(it, None)
                    if nxt:
                        futs[ex.submit(bajar_dia, ee, nxt, g)] = nxt
                break
    resto = len(falta) - hechos
    print(f'tanda: {hechos:,} días en {time.time() - t0:.0f} s; errores {len(errores)}; '
          f'{"completo" if resto - len(errores) <= 0 and not errores else f"faltan {resto:,}"}', flush=True)
    for i, e in errores[:10]:
        print(f'  error {i}: {e}', flush=True)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--bajar', action='store_true')
    ap.add_argument('--hasta-min', type=float, default=9.0)
    ap.add_argument('--mensual', action='store_true')
    ap.add_argument('--procesar', action='store_true')
    a = ap.parse_args()
    if a.bajar:
        return bajar_diario(a.hasta_min)
    if a.mensual:
        return bajar_compuestos()
    if a.procesar:
        return procesar()
    return 0


# ── compuestos mensual y anual ────────────────────────────────────────────────

def bajar_compuestos() -> int:
    import ee
    from gee_export_activity_rasters import authenticate
    from gee_export_clima_diario import bajar
    authenticate()
    g = grilla()
    out = DIR / 'compuestos'
    out.mkdir(parents=True, exist_ok=True)
    trabajos = []
    m = ee.ImageCollection(COL_MENSUAL)
    for idx in m.aggregate_array('system:index').getInfo():
        trabajos.append(('mensual', idx, ee.Image(f'{COL_MENSUAL}/{idx}').select(['avg_rad', 'cf_cvg'])
                         .unmask(-999).toFloat()))
    for col in COL_ANUAL:
        try:
            ids = ee.ImageCollection(col).aggregate_array('system:index').getInfo()
        except Exception as e:  # noqa: BLE001
            print(f'{col}: no disponible ({str(e)[:60]})', flush=True)
            continue
        for idx in ids:
            trabajos.append((f'anual_{col[-3:].lower()}', idx, ee.Image(f'{col}/{idx}')
                             .select(['median_masked', 'average_masked', 'cf_cvg']).unmask(-999).toFloat()))

    def uno(t):
        tipo, idx, img = t
        dest = out / f'{tipo}_{idx}.npz'
        if dest.exists():
            return
        r = bajar(img, g)
        np.savez_compressed(dest, **{n: r[n] for n in r.dtype.names},
                            grilla=np.array([g['x0'], g['dx'], g['y0'], g['dy'], g['ancho'], g['alto']]))
    with ThreadPoolExecutor(HILOS) as ex:
        list(ex.map(uno, trabajos))
    print(f'compuestos: {len(trabajos)} imágenes ({sum(1 for t in trabajos if t[0] == "mensual")} mensuales)', flush=True)
    return 0


# ── procesamiento ─────────────────────────────────────────────────────────────

def _cargar_anuales() -> tuple[list[str], np.ndarray]:
    arch = sorted((DIR / 'compuestos').glob('anual_*.npz'))
    nombres, med = [], []
    for p in arch:
        d = np.load(p)
        nombres.append(p.stem)
        med.append(np.where(d['median_masked'] < -900, np.nan, d['median_masked']))
    return nombres, np.array(med)


def procesar() -> int:
    import io

    import pandas as pd
    from pyproj import Transformer
    from scipy.spatial import cKDTree

    from build_hexoma import DB, connect, log
    t0 = time.time()
    g = grilla()
    alto, ancho = g['alto'], g['ancho']
    nombres, med = _cargar_anuales()
    # sólo Misiones + 5 km (el rectángulo incluye Encarnación y pueblos de Brasil)
    import shapely
    conn = connect(DB)
    cur0 = conn.cursor()
    cur0.execute('SELECT ST_AsBinary(ST_Buffer(ST_Union(geom), 0.05)) FROM limites WHERE territorio_id = 1')
    mis = shapely.from_wkb(bytes(cur0.fetchone()[0]))
    shapely.prepare(mis)
    ff0, cc0 = np.indices((alto, ancho))
    dentro = shapely.contains_xy(mis, g['x0'] + (cc0 + 0.5) * PASO, g['y0'] - (ff0 + 0.5) * PASO)
    iluminado = (np.nan_to_num(med, nan=0) > UMBRAL_LUZ).any(axis=0) & dentro
    filas_, cols_ = np.nonzero(iluminado)
    pid_lit = filas_ * ancho + cols_
    log(f'compuestos anuales: {len(nombres)} ({nombres[0]} … {nombres[-1]}); píxeles en Misiones + 5 km: '
        f'{int(dentro.sum()):,}; iluminados (mediana anual > {UMBRAL_LUZ} en algún año): {len(pid_lit):,}')

    # tabla de píxeles (todos) + asentamiento BAHRA más cercano a ≤ 3 km para los iluminados
    ff, cc = np.indices((alto, ancho))
    lon = g['x0'] + (cc.ravel() + 0.5) * PASO
    lat = g['y0'] - (ff.ravel() + 0.5) * PASO
    pix = pd.DataFrame({'pixel_id': np.arange(alto * ancho), 'fila': ff.ravel(), 'col': cc.ravel(), 'lon': lon, 'lat': lat,
                        'iluminado': iluminado.ravel(), 'en_misiones': dentro.ravel()})
    for k, n in enumerate(nombres):
        pix[f'mediana_{n.split("_", 2)[-1]}'] = med[k].ravel()
    asen = pd.read_sql('SELECT gid, nombre, ST_X(geom) lon, ST_Y(geom) lat FROM infra.asentamientos', conn)
    utm = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)
    d, j = cKDTree(np.column_stack(utm.transform(asen.lon.values, asen.lat.values))).query(
        np.column_stack(utm.transform(pix.lon.values, pix.lat.values)), distance_upper_bound=3000)
    pix['asentamiento_gid'] = np.where(np.isfinite(d), asen.gid.values[np.minimum(j, len(asen) - 1)], np.nan)
    pix.loc[~pix.iluminado, 'asentamiento_gid'] = np.nan

    # serie diaria de los píxeles iluminados → Parquet por año (pyarrow); agregado por asentamiento con una matriz
    # dispersa píxel × asentamiento (válido = con dato, calidad 0/1, despejado: bits 6–7 de QF_Cloud_Mask ≤ 01)
    import pyarrow as pa
    import pyarrow.parquet as pqw
    from scipy.sparse import csr_matrix
    pq = DIR / 'parquet'
    pq.mkdir(parents=True, exist_ok=True)
    asen_lit = pix.loc[pid_lit, 'asentamiento_gid'].values
    con_asen = ~np.isnan(asen_lit)
    gids = np.unique(asen_lit[con_asen]).astype(int)
    col_a = np.searchsorted(gids, asen_lit[con_asen].astype(int))
    S = csr_matrix((np.ones(con_asen.sum()), (np.flatnonzero(con_asen), col_a)), shape=(len(pid_lit), len(gids)))
    n_pix_asen = np.asarray(S.sum(axis=0)).ravel()
    agregados = []
    dias_ok = 0
    for anio_dir in sorted(p for p in DIR.iterdir() if p.is_dir() and p.name.isdigit()):
        archivos = sorted(anio_dir.glob('vnp46a2_*.npz'))
        if not archivos:
            continue
        nd = len(archivos)
        R = np.empty((nd, len(pid_lit)), np.uint16)
        Q = np.empty((nd, len(pid_lit)), np.uint8)
        N = np.empty((nd, len(pid_lit)), np.uint16)
        fechas = []
        for k, f in enumerate(archivos):
            z = np.load(f)
            R[k], Q[k], N[k] = z['rad'].ravel()[pid_lit], z['qf'].ravel()[pid_lit], z['nubes'].ravel()[pid_lit]
            fechas.append(dt.datetime.strptime(f.stem[-8:], '%Y%m%d').date())
        dias_ok += nd
        tabla_ = pa.table({'pixel_id': np.tile(pid_lit.astype(np.int32), nd),
                           'fecha': pa.array(np.repeat(np.array(fechas, dtype='datetime64[D]'), len(pid_lit))),
                           'rad_x10': R.ravel(), 'qf': Q.ravel(), 'nubes': N.ravel()})
        pqw.write_table(tabla_, pq / f'vnp46a2_{anio_dir.name}.parquet', compression='zstd')
        del tabla_
        V = (R != 65535) & (Q <= 1) & (N != 65535) & (((N.astype(np.int32) >> 6) & 3) <= 1)
        rad = np.where(V, R / 10.0, 0.0)
        n_val = V.astype(np.float32) @ S
        suma = rad @ S
        n_val, suma = np.asarray(n_val), np.asarray(suma)
        dd = pd.DataFrame({'asentamiento_gid': np.tile(gids, nd), 'fecha': np.repeat(fechas, len(gids)),
                           'n_pixeles': np.tile(n_pix_asen, nd).astype(int), 'n_validos': n_val.ravel().astype(int),
                           'rad_suma': suma.ravel()})
        dd['rad_media'] = np.where(dd.n_validos > 0, dd.rad_suma / dd.n_validos.clip(lower=1), np.nan)
        agregados.append(dd)
        log(f'  {anio_dir.name}: {nd} días, {nd * len(pid_lit):,} valores; válidos {V.mean():.1%} ({time.time() - t0:.0f} s)')
        del R, Q, N, V, rad
    agg = pd.concat(agregados, ignore_index=True)

    # mensual (VCMSLCFG) de los píxeles iluminados
    mens = []
    for p in sorted((DIR / 'compuestos').glob('mensual_*.npz')):
        z = np.load(p)
        mes = dt.datetime.strptime(p.stem.split('_')[1][:6], '%Y%m').date()
        r, c = z['avg_rad'].ravel()[pid_lit], z['cf_cvg'].ravel()[pid_lit]
        mens.append(pd.DataFrame({'pixel_id': pid_lit, 'mes': mes, 'avg_rad': np.where(r < -900, np.nan, r),
                                  'cf_cvg': np.where(c < -900, np.nan, c)}))
    mens = pd.concat(mens, ignore_index=True) if mens else pd.DataFrame()

    cur = conn.cursor()

    def copiar(df, t, ddl, com):
        cur.execute(f'DROP TABLE IF EXISTS {t}')
        cur.execute(f'CREATE TABLE {t} ({ddl})')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
        cur.execute(f'COMMENT ON TABLE {t} IS %s', (com,))

    med_cols = [c for c in pix.columns if c.startswith('mediana_')]
    pix['asentamiento_gid'] = pix.asentamiento_gid.astype('Int64')
    copiar(pix[['pixel_id', 'fila', 'col', 'lon', 'lat', 'iluminado', 'en_misiones', 'asentamiento_gid', *med_cols]], 'infra.luz_pixel',
           'pixel_id int PRIMARY KEY, fila int, col int, lon double precision, lat double precision, iluminado boolean, en_misiones boolean, '
           'asentamiento_gid int, ' + ', '.join(f'{c} real' for c in med_cols),
           f'Píxeles de 15" (VIIRS/Black Marble) del recorte de Misiones; iluminado = mediana anual > {UMBRAL_LUZ} '
           'nW·cm⁻²·sr⁻¹ en algún año; asentamiento BAHRA más cercano a ≤ 3 km (luz.py)')
    cur.execute('ALTER TABLE infra.luz_pixel ADD COLUMN geom geometry(Point, 4326)')
    cur.execute('UPDATE infra.luz_pixel SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)')
    cur.execute('CREATE INDEX ON infra.luz_pixel USING gist (geom)')
    # celda h3 → píxel que contiene su centro (como clima.hex_pixel)
    cur.execute('DROP TABLE IF EXISTS infra.luz_h3_pixel')
    cur.execute(f"""CREATE TABLE infra.luz_h3_pixel AS
        SELECT h.h3, (floor(({g['y0']} - (h3_cell_to_lat_lng(h.h3))[1]) / {PASO})::int * {ancho}
                      + floor(((h3_cell_to_lat_lng(h.h3))[0] - {g['x0']}) / {PASO})::int) AS pixel_id
        FROM hexagonos h WHERE h.territorio_id = 1""")
    cur.execute("COMMENT ON TABLE infra.luz_h3_pixel IS 'Celda h3 de Misiones → píxel VIIRS de 15\" que contiene su centro'")
    copiar(agg, 'infra.luz_asentamiento_diaria',
           'asentamiento_gid int, fecha date, n_pixeles int, n_validos int, rad_suma real, rad_media real',
           'Black Marble diario (VNP46A2) por asentamiento BAHRA: píxeles iluminados asignados, píxeles válidos (calidad '
           'alta y despejado) y radiancia de los válidos (luz.py). Sin detección de cortes: sólo datos.')
    cur.execute('CREATE INDEX ON infra.luz_asentamiento_diaria (asentamiento_gid, fecha)')
    if len(mens):
        copiar(mens, 'infra.luz_mensual', 'pixel_id int, mes date, avg_rad real, cf_cvg real',
               'VIIRS DNB mensual sin luz difusa (VCMSLCFG) de los píxeles iluminados (luz.py)')
        cur.execute('CREATE INDEX ON infra.luz_mensual (pixel_id, mes)')
    for t, n, nota, url in [
        ('infra.luz_asentamiento_diaria', len(agg), f'{dias_ok} días de VNP46A2', COL_DIARIA),
        ('infra.luz_mensual', len(mens), 'VCMSLCFG', COL_MENSUAL)]:
        cur.execute("""INSERT INTO infra.fuentes VALUES (%s, 'NASA Black Marble / NOAA VIIRS (Google Earth Engine)', %s,
                       current_date::text, now(), %s, %s) ON CONFLICT (tabla) DO UPDATE SET cargado=now(),
                       n=EXCLUDED.n, nota=EXCLUDED.nota""", (t, url, n, nota))
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s: {dias_ok} días, {len(agg):,} filas por asentamiento')
    return 0


if __name__ == '__main__':
    sys.exit(main())
