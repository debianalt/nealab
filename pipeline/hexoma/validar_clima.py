"""Hexoma A1 — valida ERA5-Land y CHIRPS v3 contra estaciones meteorológicas del NEA.

Estaciones: GHCN-Daily de NOAA (abierto; SMN de Argentina, DMH de Paraguay e INMET/aeropuertos
de Brasil cerca del límite), bajadas a %HEXOMA_FUENTES%/estaciones/ghcnd/. Cada estación se compara
con el píxel que la contiene (el mismo criterio que una celda), en el período 1991–2025.

Estaciones extra (p. ej. INTA SIGA: Cerro Azul, Oberá, Bernardo de Irigoyen, que GHCN no tiene y
que el sitio de SIGA no deja bajar en forma automática: responde 403): un CSV por estación en
%HEXOMA_FUENTES%/estaciones/extra/<nombre>.csv con columnas fecha (AAAA-MM-DD), tmin, tmax, p
(°C, °C, mm; vacío = sin dato) y un <nombre>.json con {"lat": …, "lng": …, "elev": …}.

Métricas (sólo años o campañas con ≥ 95 % de días válidos en la estación):
  diario       Tmin y Tmax: sesgo (modelo − estación), MAE y r
  por año      días con Tmin < 0 °C y con Tmin ≤ 3 °C (media de cada serie y r de las series)
  por campaña  grados-día base 10 y lluvia jul–jun; días de lluvia ≥ 1 mm (CHIRPS)
No se corrige el sesgo: se mide y se informa (decisión A1).

Salida: tabla clima.validacion y pipeline/hexoma/validacion_clima.md.

Uso (desde neahub/):
    python pipeline/hexoma/validar_clima.py
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from clima_a_pixel import DIARIO, archivos  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
EST = FUENTES_DIR / 'estaciones'
GHCN = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{}.dly'
INFORME = Path(__file__).resolve().parent / 'validacion_clima.md'
DESDE, HASTA = 1991, 2025

# id GHCN → (nombre, lat, lng, elevación m)
ESTACIONES = {
    'ARM00087178': ('Posadas', -27.386, -55.971, 131),
    'BR002554002': ('Iguazú Aero', -25.733, -54.467, 279),
    'BRM00083827': ('Foz do Iguaçu (Cataratas)', -25.596, -54.487, 240),
    'PA000086297': ('Encarnación (PY)', -27.300, -55.900, 91),
    'PAM00086285': ('Capitán Meza (PY)', -26.933, -55.200, 248),
    'PA000086248': ('Ciudad del Este (PY)', -25.533, -54.600, 196),
    'ARM00087166': ('Corrientes', -27.446, -58.762, 62),
    'ARM00087289': ('Paso de los Libres', -29.689, -57.152, 70),
    'AR000000011': ('Monte Caseros', -30.267, -57.650, 53),
    'AR000000008': ('Curuzú Cuatiá', -29.783, -57.983, 70),
    'AR000087155': ('Resistencia', -27.450, -59.050, 53),
    'ARM00087148': ('Presidencia Roque Sáenz Peña', -26.733, -60.483, 93),
    'ARM00087162': ('Formosa', -26.213, -58.228, 59),
    'AR000087078': ('Las Lomitas', -24.700, -60.583, 130),
    'AR000087065': ('Rivadavia (Salta)', -24.167, -62.900, 205),
    'AR000087270': ('Reconquista (Santa Fe)', -29.183, -59.700, 53),
    'AR000087257': ('Ceres (Santa Fe)', -29.883, -61.950, 88),
}


# ---------------------------------------------------------------- estaciones
def leer_dly(path: Path) -> pd.DataFrame:
    filas = []
    for line in path.read_text(encoding='latin-1').splitlines():
        elem = line[17:21]
        if elem not in ('TMAX', 'TMIN', 'PRCP'):
            continue
        y, m = int(line[11:15]), int(line[15:17])
        for d in range(31):
            s = line[21 + d * 8: 29 + d * 8]
            v, qflag = int(s[:5]), s[6]
            if v == -9999 or qflag.strip():
                continue
            filas.append((y, m, d + 1, elem, v / 10))
    df = pd.DataFrame(filas, columns=['y', 'm', 'd', 'elem', 'v'])
    df = df[pd.to_datetime(dict(year=df.y, month=df.m, day=df.d), errors='coerce').notna()]
    df['fecha'] = pd.to_datetime(dict(year=df.y, month=df.m, day=df.d))
    w = df.pivot_table(index='fecha', columns='elem', values='v', aggfunc='first')
    return w.rename(columns={'TMAX': 'tmax', 'TMIN': 'tmin', 'PRCP': 'p'})


def estaciones() -> dict[str, tuple[str, float, float, float, pd.DataFrame]]:
    (EST / 'ghcnd').mkdir(parents=True, exist_ok=True)
    out = {}
    for sid, (nombre, lat, lng, elev) in ESTACIONES.items():
        f = EST / 'ghcnd' / f'{sid}.dly'
        if not f.exists():
            subprocess.run(['curl', '-s', '-f', '-o', str(f), GHCN.format(sid)], check=True)
        out[sid] = (nombre, lat, lng, elev, leer_dly(f))
    for f in sorted((EST / 'extra').glob('*.csv')) if (EST / 'extra').exists() else []:
        meta = json.loads(f.with_suffix('.json').read_text(encoding='utf-8'))
        d = pd.read_csv(f, parse_dates=['fecha']).set_index('fecha')
        out[f.stem] = (f.stem, meta['lat'], meta['lng'], meta.get('elev'), d[['tmin', 'tmax', 'p']])
    log(f'{len(out)} estaciones')
    return out


# ---------------------------------------------------------------- modelos en el píxel de cada estación
def series_modelo(fuente: str, variables: list[str], puntos: dict) -> dict[str, pd.DataFrame]:
    """Serie diaria del píxel que contiene cada estación (lee la ventana entera de cada año)."""
    res = {k: [] for k in puntos}
    for f in archivos(fuente):
        z = np.load(f)
        x0, dx, y0, dy, ancho, alto = (float(v) for v in z['grilla'][:6])
        fechas = pd.DatetimeIndex(z['fechas'])
        datos = {v: z[v] for v in variables}
        for k, (lat, lng) in puntos.items():
            c, r = int(np.floor((lng - x0) / dx)), int(np.floor((lat - y0) / dy))
            if not (0 <= c < ancho and 0 <= r < alto):
                continue
            res[k].append(pd.DataFrame({v: datos[v][:, r, c] for v in variables}, index=fechas))
    return {k: pd.concat(v) for k, v in res.items() if v}


def campania(idx: pd.DatetimeIndex) -> np.ndarray:
    return np.where(idx.month >= 7, idx.year, idx.year - 1)


def metricas(sid, nombre, est: pd.DataFrame, e5: pd.DataFrame | None, ch: pd.DataFrame | None) -> list[dict]:
    filas = []
    est = est[(est.index.year >= DESDE) & (est.index.year <= HASTA)]

    def fila(metrica, n, v_est, v_mod, sesgo=None, mae=None, r=None, fuente='era5'):
        filas.append(dict(estacion=sid, nombre=nombre, fuente=fuente, metrica=metrica, n=int(n),
                          valor_estacion=v_est, valor_modelo=v_mod, sesgo=sesgo, mae=mae, r=r))

    if e5 is not None:
        j = est.join(e5, how='inner', rsuffix='_m')
        for v in ('tmin', 'tmax'):
            if v not in j:
                continue
            x = j[[v, f'{v}_m']].dropna()
            if len(x) > 365:
                d = x[f'{v}_m'] - x[v]
                fila(f'{v} diaria (°C)', len(x), x[v].mean(), x[f'{v}_m'].mean(), d.mean(), d.abs().mean(),
                     np.corrcoef(x[v], x[f'{v}_m'])[0, 1])
        if 'tmin' in j:
            x = j[['tmin', 'tmin_m']].dropna()
            g = x.groupby(x.index.year)
            ok = g.size() >= 0.95 * 365
            for umbral, rot in ((0, 'días con Tmin < 0 °C por año'), (3, 'días con Tmin ≤ 3 °C por año')):
                f = (lambda s: (s < 0).sum()) if umbral == 0 else (lambda s: (s <= 3).sum())
                a = g.tmin.apply(f)[ok]
                b = g.tmin_m.apply(f)[ok]
                if len(a) >= 5:
                    r = np.corrcoef(a, b)[0, 1] if a.std() > 0 and b.std() > 0 else None
                    fila(rot, len(a), a.mean(), b.mean(), (b - a).mean(), (b - a).abs().mean(), r)
        if {'tmin', 'tmax'} <= set(j):
            x = j[['tmin', 'tmax', 'tmin_m', 'tmax_m']].dropna()
            x = x.assign(c=campania(x.index))
            g = x.groupby('c')
            ok = g.size() >= 0.95 * 365
            gdd = lambda tx, tn: np.maximum((tx + tn) / 2 - 10, 0).sum()
            a = g.apply(lambda s: gdd(s.tmax, s.tmin))[ok]
            b = g.apply(lambda s: gdd(s.tmax_m, s.tmin_m))[ok]
            if len(a) >= 5:
                fila('grados-día base 10 por campaña (°C·d)', len(a), a.mean(), b.mean(), (b - a).mean(),
                     (b - a).abs().mean(), np.corrcoef(a, b)[0, 1])
    if ch is not None and 'p' in est:
        x = est[['p']].join(ch.rename(columns={'p': 'p_m'}), how='inner').dropna()
        x = x.assign(c=campania(x.index))
        g = x.groupby('c')
        ok = g.size() >= 0.95 * 365
        a, b = g.p.sum()[ok], g.p_m.sum()[ok]
        if len(a) >= 5:
            fila('lluvia por campaña (mm)', len(a), a.mean(), b.mean(), (b - a).mean(), (b - a).abs().mean(),
                 np.corrcoef(a, b)[0, 1], 'chirps')
            a2 = g.p.apply(lambda s: (s >= 1).sum())[ok]
            b2 = g.p_m.apply(lambda s: (s >= 1).sum())[ok]
            fila('días de lluvia ≥ 1 mm por campaña', len(a2), a2.mean(), b2.mean(), (b2 - a2).mean(),
                 (b2 - a2).abs().mean(), np.corrcoef(a2, b2)[0, 1], 'chirps')
            if len(x) > 365:
                fila('lluvia diaria (mm)', len(x), x.p.mean(), x.p_m.mean(), (x.p_m - x.p).mean(),
                     (x.p_m - x.p).abs().mean(), np.corrcoef(x.p, x.p_m)[0, 1], 'chirps')
    return filas


def main() -> None:
    t0 = time.time()
    est = estaciones()
    puntos = {k: (v[1], v[2]) for k, v in est.items()}
    e5 = series_modelo('era5', ['tmin', 'tmax'], puntos)
    for k in e5:
        e5[k] = e5[k] - 273.15
    log(f'ERA5-Land en {len(e5)} píxeles de estación')
    ch = series_modelo('chirps', ['p'], puntos)
    log(f'CHIRPS en {len(ch)} píxeles de estación')
    filas = []
    for sid, (nombre, lat, lng, elev, d) in est.items():
        filas += metricas(sid, nombre, d, e5.get(sid), ch.get(sid))
    v = pd.DataFrame(filas)

    # altitud del píxel ERA5 frente a la de la estación
    import psycopg2  # noqa: F401
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS clima.validacion')
        cur.execute("""CREATE TABLE clima.validacion (estacion text, nombre text, fuente text, metrica text, n int,
                       valor_estacion float8, valor_modelo float8, sesgo float8, mae float8, r float8)""")
        buf = io.StringIO()
        v.to_csv(buf, index=False, header=False, na_rep='')
        buf.seek(0)
        cur.copy_expert(f"COPY clima.validacion ({', '.join(v.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        cur.execute("COMMENT ON TABLE clima.validacion IS 'ERA5-Land y CHIRPS v3 contra estaciones GHCN-Daily, "
                    f"{DESDE}-{HASTA}, píxel que contiene la estación. sesgo = modelo − estación. "
                    "pipeline/hexoma/validar_clima.py'")
    informe(v, est)
    log(f'listo en {time.time() - t0:.0f} s')


def informe(v: pd.DataFrame, est: dict) -> None:
    def f(x, nd=1):
        return '' if pd.isna(x) else f'{x:,.{nd}f}'.replace(',', ' ').replace('.', ',')

    L = ['# Validación del clima del Hexoma contra estaciones', '',
         f'Generado por `pipeline/hexoma/validar_clima.py` el {time.strftime("%d-%m-%Y")}. Período {DESDE}–{HASTA}; '
         'cada estación contra el píxel que la contiene (ERA5-Land 0,1°, CHIRPS v3 0,05°). '
         '**Sesgo = modelo − estación.** Estaciones de GHCN-Daily (NOAA). En A1 el sesgo se mide, '
         'no se corrige.', '',
         'Sólo entran años y campañas con al menos 95 % de días válidos en la estación.', '']
    for metrica in v.metrica.unique():
        s = v[v.metrica == metrica]
        L += [f'## {metrica[0].upper() + metrica[1:]}', '',
              '| Estación | n | Estación | Modelo | Sesgo | MAE | r |', '|---|---:|---:|---:|---:|---:|---:|']
        for _, r in s.iterrows():
            L.append(f'| {r.nombre} | {r.n} | {f(r.valor_estacion)} | {f(r.valor_modelo)} | {f(r.sesgo)} | '
                     f'{f(r.mae)} | {f(r.r, 2)} |')
        L += ['', f'Mediana del sesgo: {f(s.sesgo.median())}; mediana de r: {f(s.r.median(), 2)}.', '']
    falta = [k for k, x in est.items() if x[4].empty]
    L += ['## Huecos', '',
          '- Las sierras de Misiones (Oberá, Bernardo de Irigoyen, Cerro Azul) no están en GHCN-Daily. INTA SIGA '
          'tiene esas series, pero su sitio responde 403 a la descarga automática. Para sumarlas: bajarlas a mano '
          'y dejarlas en `%HEXOMA_FUENTES%/estaciones/extra/` (formato en el docstring del script).']
    if falta:
        L.append(f'- Estaciones sin datos: {", ".join(falta)}.')
    INFORME.write_text('\n'.join(L) + '\n', encoding='utf-8')
    log(f'informe: {INFORME}')


if __name__ == '__main__':
    main()
