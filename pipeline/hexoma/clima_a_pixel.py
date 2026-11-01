"""Hexoma A1 — indicadores agroclimáticos por píxel de origen (CHIRPS v3 y ERA5-Land), NEA AR.

Lee los .npz diarios de gee_export_clima_diario.py (%HEXOMA_FUENTES%/clima/diario/)
y escribe parquets en %HEXOMA_FUENTES%/clima/derivados/, que carga build_hexoma_clima.py.

Unidad espacial: el PÍXEL DE ORIGEN de cada fuente, no el hexágono. Cada hexágono lee el píxel que
contiene su centro (decisión del 1-nov-2026: no se interpola; un píxel de CHIRPS cubre ~70 celdas,
uno de ERA5-Land ~1.100). Sólo se procesan los píxeles que contienen algún centro de celda.
pixel_id = fila_global × 100000 + columna_global, en la grilla global de la fuente.

Unidades de tiempo:
- campaña agrícola jul–jun (campania = año de inicio: 2023 = jul-2023 a jun-2024);
- estación meteorológica (DEF/MAM/JJA/SON; anio_estacion de DEF = año de diciembre);
- heladas por AÑO CALENDARIO (el invierno no se parte entre dos campañas).
Sólo se guardan grupos completos (todos los días presentes).

ET0: FAO-56 Penman-Monteith de referencia (pasto), diaria, con los datos de ERA5-Land:
Tmax/Tmin, ea del punto de rocío medio, viento a 2 m desde el viento medio a 10 m (módulo del
vector medio diario: subestima algo la velocidad), presión de superficie (γ y altitud para Rso) y
radiación solar incidente. Rn = 0,77·Rs − Rnl (ec. 38–39), G = 0. No se usa la radiación neta de
ERA5: depende del albedo real de la superficie, no del pasto de referencia.

Uso (desde neahub/):
    python pipeline/hexoma/clima_a_pixel.py                 # todo
    python pipeline/hexoma/clima_a_pixel.py --fuente chirps
    python pipeline/hexoma/clima_a_pixel.py --test          # sólo la prueba de ET0 (FAO-56, ej. 18)
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
CLIMA = FUENTES_DIR / 'clima'
DIARIO = CLIMA / 'diario'
DERIV = CLIMA / 'derivados'
DB = 'postgresql://postgres@localhost:5432/hexoma'

ESTACION = {12: 'DEF', 1: 'DEF', 2: 'DEF', 3: 'MAM', 4: 'MAM', 5: 'MAM',
            6: 'JJA', 7: 'JJA', 8: 'JJA', 9: 'SON', 10: 'SON', 11: 'SON'}


def log(msg: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {msg}', flush=True)


# ---------------------------------------------------------------- ET0 FAO-56
def e0(t):
    """Presión de vapor de saturación (kPa), FAO-56 ec. 11."""
    return 0.6108 * np.exp(17.27 * t / (t + 237.3))


def et0_fao56(tmax, tmin, ea, u2, p_kpa, rs, lat_rad, doy, z):
    """ET0 de referencia (mm/día), FAO-56 ec. 6, con Rn de las ec. 21–39 y G = 0."""
    t = (tmax + tmin) / 2
    delta = 4098 * e0(t) / (t + 237.3) ** 2
    gamma = 0.000665 * p_kpa
    es = (e0(tmax) + e0(tmin)) / 2
    dr = 1 + 0.033 * np.cos(2 * np.pi * doy / 365)
    dec = 0.409 * np.sin(2 * np.pi * doy / 365 - 1.39)
    ws = np.arccos(np.clip(-np.tan(lat_rad) * np.tan(dec), -1, 1))
    ra = 24 * 60 / np.pi * 0.082 * dr * (ws * np.sin(lat_rad) * np.sin(dec)
                                         + np.cos(lat_rad) * np.cos(dec) * np.sin(ws))
    rso = (0.75 + 2e-5 * z) * ra
    ratio = np.clip(np.where(rso > 0, rs / rso, 1.0), 0.25, 1.0)
    rnl = (4.903e-9 * ((tmax + 273.16) ** 4 + (tmin + 273.16) ** 4) / 2
           * (0.34 - 0.14 * np.sqrt(np.maximum(ea, 0))) * (1.35 * ratio - 0.35))
    rn = 0.77 * rs - rnl
    et0 = (0.408 * delta * rn + gamma * 900 / (t + 273) * u2 * (es - ea)) / (delta + gamma * (1 + 0.34 * u2))
    return np.maximum(et0, 0)


def test_et0() -> None:
    # FAO-56, ejemplo 18: Bruselas, 6 de julio, 50°48'N, 100 m. ET0 = 3,9 mm/día.
    v = et0_fao56(21.5, 12.3, 1.409, 2.078, 100.1, 22.07, np.deg2rad(50.8), 187, 100)
    assert abs(v - 3.9) < 0.05, v
    log(f'ET0 FAO-56 ejemplo 18: {v:.2f} mm/día (esperado 3,9) OK')


# ---------------------------------------------------------------- grillas
def centros() -> pd.DataFrame:
    f = CLIMA / 'centros.parquet'
    if f.exists():
        return pd.read_parquet(f)
    import psycopg2
    with psycopg2.connect(DB) as c:
        df = pd.read_sql("""SELECT DISTINCT h3::text AS h3,
                                   ST_Y(h3_cell_to_geometry(h3)) AS lat, ST_X(h3_cell_to_geometry(h3)) AS lng
                            FROM hexagonos""", c)
    f.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(f, index=False)
    log(f'centros: {len(df):,} celdas')
    return df


def archivos(fuente: str) -> list[Path]:
    fs = sorted((DIARIO / fuente).glob(f'{fuente}_*.npz'))
    fs = [f for f in fs if '.tmp' not in f.name]
    assert fs, f'no hay {fuente} en {DIARIO}: correr pipeline/gee_export_clima_diario.py'
    return fs


def grilla(fuente: str) -> tuple[float, float, float, float]:
    """(x0, dx, y0, dy) del origen GLOBAL de la grilla de la fuente (el pixel_id es global)."""
    g = np.load(archivos(fuente)[0])['grilla']
    return float(g[6]), float(g[1]), float(g[7]), float(g[3])


def filas_cols(lat, lng, g):
    x0, dx, y0, dy = g
    return np.floor((lat - y0) / dy).astype(np.int64), np.floor((lng - x0) / dx).astype(np.int64)


def pixeles(fuente: str, cen: pd.DataFrame, fuente_grilla: str | None = None):
    g = grilla(fuente_grilla or fuente)
    fila, col = filas_cols(cen.lat.values, cen.lng.values, g)
    pid = fila * 100000 + col
    hp = pd.DataFrame({'h3': cen.h3.values, 'pixel_id': pid})
    u = np.unique(pid)
    x0, dx, y0, dy = g
    pix = pd.DataFrame({'pixel_id': u, 'fila': u // 100000, 'col': u % 100000})
    pix['lng'] = x0 + (pix.col + 0.5) * dx
    pix['lat'] = y0 + (pix.fila + 0.5) * dy
    return g, hp, pix


def ventana(z, g, pix: pd.DataFrame):
    """Índices (filas, columnas) locales del archivo para los píxeles pedidos."""
    x0, dx, y0, dy, ancho, alto = (float(v) for v in z['grilla'][:6])
    off_c = int(round((x0 - g[0]) / g[1]))
    off_f = int(round((y0 - g[2]) / g[3]))
    cc = pix.col.values - off_c
    ff = pix.fila.values - off_f
    assert (cc >= 0).all() and (cc < ancho).all() and (ff >= 0).all() and (ff < alto).all()
    return ff, cc


# ---------------------------------------------------------------- agregación
def grupos(fechas: pd.DatetimeIndex):
    camp = np.where(fechas.month >= 7, fechas.year, fechas.year - 1)
    est = np.array([ESTACION[m] for m in fechas.month])
    est_anio = np.where(fechas.month <= 2, fechas.year - 1, fechas.year)
    return camp, est, est_anio


def racha_max(seco: np.ndarray) -> np.ndarray:
    """Racha más larga de días True por columna (días × píxeles)."""
    cur = np.zeros(seco.shape[1], np.int32)
    mx = np.zeros(seco.shape[1], np.int32)
    for d in range(seco.shape[0]):
        cur = (cur + 1) * seco[d]
        np.maximum(mx, cur, out=mx)
    return mx


def agregar(fechas: pd.DatetimeIndex, claves: list[np.ndarray], nombres: list[str], metricas: dict,
            pix_ids: np.ndarray, completo: callable) -> pd.DataFrame:
    """metricas: nombre → función(slice de días) → vector por píxel. Grupos = combinaciones de claves."""
    k = pd.MultiIndex.from_arrays(claves, names=nombres)
    df_k = pd.DataFrame({'i': np.arange(len(fechas))}, index=k)
    out = []
    nivel = 0 if len(nombres) == 1 else list(range(len(nombres)))
    for key, sub in df_k.groupby(level=nivel, sort=True):
        idx = sub.i.values
        if not completo(key, fechas[idx]):
            continue
        row = {n: (key[j] if isinstance(key, tuple) else key) for j, n in enumerate(nombres)}
        cols = {m: f(idx) for m, f in metricas.items()}
        d = pd.DataFrame({'pixel_id': pix_ids, **cols})
        for n, v in row.items():
            d[n] = v
        d['n_dias'] = len(idx)
        out.append(d)
    return pd.concat(out, ignore_index=True)


def completo_por_calendario(nombres):
    """Un grupo es completo si tiene todos los días calendario que le corresponden."""
    def f(key, fs: pd.DatetimeIndex) -> bool:
        key = key if isinstance(key, tuple) else (key,)
        kv = dict(zip(nombres, key))
        if 'campania' in kv:
            ini, fin = pd.Timestamp(kv['campania'], 7, 1), pd.Timestamp(kv['campania'] + 1, 6, 30)
        elif 'anio' in kv:
            ini, fin = pd.Timestamp(kv['anio'], 1, 1), pd.Timestamp(kv['anio'], 12, 31)
        else:
            y, e = kv['anio_estacion'], kv['estacion']
            ini = {'DEF': pd.Timestamp(y, 12, 1), 'MAM': pd.Timestamp(y, 3, 1),
                   'JJA': pd.Timestamp(y, 6, 1), 'SON': pd.Timestamp(y, 9, 1)}[e]
            fin = (ini + pd.DateOffset(months=3)) - pd.Timedelta(days=1)
        return len(fs) == (fin - ini).days + 1 and fs.min() == ini and fs.max() == fin
    return f


# ---------------------------------------------------------------- CHIRPS
def cargar_diario(fuente: str, g, pix: pd.DataFrame, variables: list[str]) -> tuple[pd.DatetimeIndex, dict]:
    fechas, datos = [], {v: [] for v in variables}
    for f in archivos(fuente):
        z = np.load(f)
        ff, cc = ventana(z, g, pix)
        for v in variables:
            datos[v].append(z[v][:, ff, cc].astype(np.float32))
        fechas += list(z['fechas'])
    fi = pd.DatetimeIndex(fechas)
    assert fi.is_monotonic_increasing and not fi.duplicated().any(), fuente
    log(f'  {fuente}: {len(fi):,} días ({fi.min().date()} a {fi.max().date()}), {len(pix):,} píxeles')
    return fi, {v: np.concatenate(x, axis=0) for v, x in datos.items()}


def chirps(cen: pd.DataFrame) -> None:
    log('CHIRPS v3')
    g, hp, pix = pixeles('chirps', cen)
    fi, d = cargar_diario('chirps', g, pix, ['p'])
    p = d['p']
    p[p < 0] = np.nan
    np.save(DERIV / 'chirps_p_diario.npy', p)
    pd.Series(fi).to_frame('fecha').to_parquet(DERIV / 'chirps_fechas.parquet')
    camp, est, est_anio = grupos(fi)
    mes = fi.month.values

    def met(seleccion=None):
        def suma(i): return np.nansum(p[i], axis=0)
        def dias1(i): return (p[i] >= 1).sum(axis=0).astype(np.int16)
        def dias50(i): return (p[i] >= 50).sum(axis=0).astype(np.int16)
        def racha(i): return racha_max(p[i] < 1).astype(np.int16)
        def racha_oct_mar(i):
            j = i[np.isin(mes[i], [10, 11, 12, 1, 2, 3])]
            return racha_max(p[j] < 1).astype(np.int16)
        def nan(i): return np.isnan(p[i]).sum(axis=0).astype(np.int16)
        m = {'p_mm': suma, 'dias_lluvia': dias1, 'dias_p50': dias50, 'racha_seca_max': racha, 'n_nulos': nan}
        if seleccion == 'campania':
            m['racha_seca_max_oct_mar'] = racha_oct_mar
        return m

    c = agregar(fi, [camp], ['campania'], met('campania'), pix.pixel_id.values, completo_por_calendario(['campania']))
    e = agregar(fi, [est_anio, est], ['anio_estacion', 'estacion'], met(), pix.pixel_id.values,
                completo_por_calendario(['anio_estacion', 'estacion']))
    pix.to_parquet(DERIV / 'pixel_chirps.parquet', index=False)
    hp.to_parquet(DERIV / 'hex_pixel_chirps.parquet', index=False)
    c.to_parquet(DERIV / 'campania_chirps.parquet', index=False)
    e.to_parquet(DERIV / 'estacion_chirps.parquet', index=False)
    log(f'CHIRPS: {len(pix):,} píxeles, {c.campania.nunique()} campañas '
        f'({c.campania.min()}–{c.campania.max()}), {len(e):,} filas de estación')


# ---------------------------------------------------------------- ERA5-Land
ERA5_VARS = ['tmax', 'tmin', 'td', 'u10', 'v10', 'sp', 'ssrd']


def era5(cen: pd.DataFrame) -> None:
    log('ERA5-Land')
    g, hp, pix = pixeles('era5', cen)
    fi, d = cargar_diario('era5', g, pix, ERA5_VARS)
    tmax, tmin, td = d['tmax'] - 273.15, d['tmin'] - 273.15, d['td'] - 273.15
    p_kpa = d['sp'] / 1000
    z = (293 - 293 * (p_kpa / 101.3) ** (1 / 5.26)) / 0.0065  # FAO-56 ec. 7 invertida
    ws10 = np.hypot(d['u10'], d['v10'])
    u2 = ws10 * 4.87 / np.log(67.8 * 10 - 5.42)
    rs = d['ssrd'] / 1e6
    lat = np.deg2rad(pix.lat.values)[None, :]
    doy = fi.dayofyear.values[:, None]
    et0 = et0_fao56(tmax, tmin, e0(td), u2, p_kpa, rs, lat, doy, z).astype(np.float32)
    del d, td, ws10, u2, rs
    for nombre, arr in (('tmax', tmax), ('tmin', tmin), ('et0', et0)):
        np.save(DERIV / f'era5_{nombre}_diario.npy', arr.astype(np.float32))
    pd.Series(fi).to_frame('fecha').to_parquet(DERIV / 'era5_fechas.parquet')
    pix['z_m'] = np.nanmean(z, axis=0)
    camp, est, est_anio = grupos(fi)
    tm = (tmax + tmin) / 2

    m = {
        'gdd10': lambda i: np.nansum(np.maximum(tm[i] - 10, 0), axis=0),
        'dias_tmax35': lambda i: (tmax[i] > 35).sum(axis=0).astype(np.int16),
        'tmax_media': lambda i: np.nanmean(tmax[i], axis=0),
        'tmin_media': lambda i: np.nanmean(tmin[i], axis=0),
        'et0_mm': lambda i: np.nansum(et0[i], axis=0),
        'n_nulos': lambda i: np.isnan(tmax[i]).sum(axis=0).astype(np.int16),
    }
    c = agregar(fi, [camp], ['campania'], m, pix.pixel_id.values, completo_por_calendario(['campania']))
    e = agregar(fi, [est_anio, est], ['anio_estacion', 'estacion'], m, pix.pixel_id.values,
                completo_por_calendario(['anio_estacion', 'estacion']))

    doyv = fi.dayofyear.values

    def primera(i):
        h = tmin[i] < 0
        r = np.where(h.any(axis=0), doyv[i][h.argmax(axis=0)], -1)
        return r.astype(np.int16)

    def ultima(i):
        h = tmin[i][::-1] < 0
        r = np.where(h.any(axis=0), doyv[i][::-1][h.argmax(axis=0)], -1)
        return r.astype(np.int16)

    mh = {
        'dias_tmin_lt0': lambda i: (tmin[i] < 0).sum(axis=0).astype(np.int16),
        'dias_tmin_lt_m2': lambda i: (tmin[i] < -2).sum(axis=0).astype(np.int16),
        'dias_tmin_le3': lambda i: (tmin[i] <= 3).sum(axis=0).astype(np.int16),
        'primera_doy': primera,
        'ultima_doy': ultima,
        'tmin_abs': lambda i: np.nanmin(tmin[i], axis=0),
    }
    h = agregar(fi, [fi.year.values], ['anio'], mh, pix.pixel_id.values, completo_por_calendario(['anio']))
    for col in ('primera_doy', 'ultima_doy'):
        h[col] = h[col].where(h[col] >= 0)  # sin helada = NULL

    # humedad del suelo: media mensual → campaña / estación ponderando por días del mes
    if True:
        sw, meses = {v: [] for v in ('sw1', 'sw2', 'sw3')}, []
        for f in archivos('era5'):
            z = np.load(f)
            ff, cc = ventana(z, g, pix)
            anio = int(f.stem.split('_')[1])
            meses += [f'{anio}-{m:02d}' for m in z['meses']]
            for v in sw:
                sw[v].append(z[v][:, ff, cc])
        a = np.concatenate([np.concatenate(sw[v], axis=0) for v in ('sw1', 'sw2', 'sw3')], axis=0)
        meses = pd.PeriodIndex(meses, freq='M')
        nm = len(meses)
        dias = meses.days_in_month.values.astype(float)
        mc = np.where(meses.month >= 7, meses.year, meses.year - 1)
        me = np.array([ESTACION[x] for x in meses.month])
        mea = np.where(meses.month <= 2, meses.year - 1, meses.year)
        for k, v in enumerate(['sw1', 'sw2', 'sw3']):
            s = a[k * nm:(k + 1) * nm].astype(float)
            for df, keys, cols in ((c, [mc], ['campania']), (e, [mea, me], ['anio_estacion', 'estacion'])):
                kk = pd.MultiIndex.from_arrays(keys)
                w = pd.DataFrame(s * dias[:, None], index=kk).groupby(level=list(range(len(keys)))).sum()
                wd = pd.Series(dias, index=kk).groupby(level=list(range(len(keys)))).sum()
                media = w.div(wd, axis=0)
                media.columns = pix.pixel_id.values
                long = media.stack().rename(v).reset_index()
                long.columns = cols + ['pixel_id', v]
                merged = df.merge(long, on=cols + ['pixel_id'], how='left')
                df[v] = merged[v].values
        log(f'  humedad del suelo: {nm} meses')
    pix.to_parquet(DERIV / 'pixel_era5.parquet', index=False)
    hp.to_parquet(DERIV / 'hex_pixel_era5.parquet', index=False)
    c.to_parquet(DERIV / 'campania_era5.parquet', index=False)
    e.to_parquet(DERIV / 'estacion_era5.parquet', index=False)
    h.to_parquet(DERIV / 'helada_era5.parquet', index=False)
    log(f'ERA5-Land: {len(pix):,} píxeles, {c.campania.nunique()} campañas '
        f'({c.campania.min()}–{c.campania.max()}), {h.anio.nunique()} años de heladas')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--fuente', choices=['chirps', 'era5'])
    ap.add_argument('--test', action='store_true')
    args = ap.parse_args()
    test_et0()
    if args.test:
        return 0
    DERIV.mkdir(parents=True, exist_ok=True)
    cen = centros()
    if args.fuente in (None, 'chirps'):
        chirps(cen)
    if args.fuente in (None, 'era5'):
        era5(cen)
    return 0


if __name__ == '__main__':
    sys.exit(main())
