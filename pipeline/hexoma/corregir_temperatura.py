"""Hexoma — corrección de Tmin y Tmax de ERA5-Land con las estaciones (heladas y calor extremo).

ERA5-Land da mínimas ~1,5 °C altas y máximas ~2,1 °C bajas en el NEA (validar_clima.py): casi no marca
heladas. Este script corrige la temperatura diaria de cada píxel con las estaciones del esquema
`estaciones` (GHCN-Daily e INTA SIGA) y elige el método por validación cruzada con una regla fijada
antes de ver los resultados.

Método
1. Función de transferencia por estación, variable y mes calendario (mapeo de cuantiles empírico):
   δ(q) = Q_estación(q) − Q_ERA5(q), en 101 cuantiles (0,005…0,995), con los días en que ambos tienen
   dato (no hace falta emparejar día a día: se comparan distribuciones). Estaciones con ≥ 150 días por
   mes y ≥ 3 años.
2. Para corregir un píxel: cada día se ubica en su cuantil q dentro de la distribución de ese píxel en ese
   mes (referencia 1991–2025) y se le suma δ(q) trasladado desde las estaciones. Fuera de [0,005; 0,995]
   se usa el δ del extremo. Se aplica a 1950–2025 suponiendo que el sesgo es estacionario.
3. Traslado de δ a un lugar (candidatos):
     a) idw: 1/d² de las 4 estaciones más cercanas a < 150 km (si no hay ninguna, la más cercana);
     b) cercana: la estación más cercana;
     c) cruda: sin corrección (línea de base).
4. Validación cruzada: cada estación con ≥ 5 años completos se predice sin ella NI las estaciones a < 10 km
   (para no hacer trampa con estaciones gemelas). Métrica: MAE de los conteos anuales en años completos
   (≥ 95 % de días): Tmin → días < 0 °C y ≤ 3 °C (suma de los dos MAE); Tmax → días > 35 °C.
   REGLA (fijada antes): por variable, se adopta el candidato de menor MAE medio entre estaciones si
   mejora ≥ 25 % el de la cruda; si no, no se publica la corrección de esa variable.
5. Helada a 5 cm (agrometeorológica): δs(q) = Q_suelo(q) − Q_abrigo(q) de las convencionales del INTA
   (observado contra observado), trasladado desde la convencional más cercana y sumado a la Tmin de abrigo
   ya corregida. Misma validación cruzada contra la helada a 5 cm de la crudas (Tmin < 0 de ERA5-Land) y
   misma regla.

Salida (%HEXOMA_FUENTES%/clima/derivados/, la carga build_hexoma_clima.py):
    campania_era5_corr.parquet, estacion_era5_corr.parquet, helada_era5_corr.parquet
    correccion_validacion.parquet, correccion_funciones.parquet, correccion_decision.json

Uso (desde neahub/):
    python pipeline/hexoma/corregir_temperatura.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from clima_a_pixel import DERIV, ESTACION, agregar, completo_por_calendario, grupos  # noqa: E402
from validar_clima import series_modelo  # noqa: E402

Q = np.linspace(0.005, 0.995, 101)
REF = (1991, 2025)
RADIO_KM = 150
VECINOS = 4
EXCLUIR_KM = 10
MEJORA = 0.25


def dist_km(lat1, lng1, lat2, lng2):
    lat1, lng1, lat2, lng2 = map(np.radians, (lat1, lng1, lat2, lng2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lng2 - lng1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------- datos
def estaciones() -> tuple[pd.DataFrame, pd.DataFrame]:
    import psycopg2  # noqa: F401
    conn = connect(DB)
    meta = pd.read_sql("""SELECT estacion_id, nombre, tipo, lat, lng, elev_m FROM estaciones.estacion
                          WHERE pct_tmin >= 50 AND n_dias >= 3 * 365""", conn)
    dia = pd.read_sql("""SELECT estacion_id, fecha, tmin, tmax, tmin_suelo FROM estaciones.v_diario_valido
                         WHERE estacion_id IN (SELECT estacion_id FROM estaciones.estacion
                                               WHERE pct_tmin >= 50 AND n_dias >= 3 * 365)""", conn,
                      parse_dates=['fecha'])
    conn.close()
    return meta, dia


def funcion(obs: np.ndarray, mod: np.ndarray) -> np.ndarray | None:
    obs, mod = obs[~np.isnan(obs)], mod[~np.isnan(mod)]
    if len(obs) < 150 or len(mod) < 150:
        return None
    return np.quantile(obs, Q) - np.quantile(mod, Q)


def ajustar(meta, dia, mod_est: dict) -> dict:
    """δ por (estación, variable, mes) con los días en que estación y ERA5 tienen dato."""
    F = {}
    for sid, d in dia.groupby('estacion_id'):
        if sid not in mod_est:
            continue
        j = d.set_index('fecha').join(mod_est[sid], how='inner', rsuffix='_m')
        for v in ('tmin', 'tmax'):
            x = j[[v, f'{v}_m']].dropna()
            if x.index.year.nunique() < 3:
                continue
            for m in range(1, 13):
                s = x[x.index.month == m]
                f = funcion(s[v].values, s[f'{v}_m'].values)
                if f is not None:
                    F[(sid, v, m)] = f
        # helada a 5 cm: observado contra observado
        x = j[['tmin', 'tmin_suelo']].dropna()
        if len(x) > 3 * 365:
            for m in range(1, 13):
                s = x[x.index.month == m]
                f = funcion(s.tmin_suelo.values, s.tmin.values)
                if f is not None:
                    F[(sid, 'suelo', m)] = f
    return F


def trasladar(F, meta, lat, lng, v, metodo, excluir=()):
    """δ(q) por mes (12 × 101) en (lat, lng) a partir de las estaciones con función para v."""
    cand = meta[~meta.estacion_id.isin(excluir)].copy()
    cand['d'] = dist_km(lat, lng, cand.lat.values, cand.lng.values)
    out = np.zeros((12, len(Q)))
    for m in range(1, 13):
        c = cand[[(s, v, m) in F for s in cand.estacion_id]].sort_values('d')
        if c.empty:
            return None
        if metodo == 'cercana' or c.d.iloc[0] >= RADIO_KM:
            out[m - 1] = F[(c.estacion_id.iloc[0], v, m)]
            continue
        c = c[c.d < RADIO_KM].head(VECINOS)
        w = 1 / np.maximum(c.d.values, 1.0) ** 2
        out[m - 1] = np.average(np.stack([F[(s, v, m)] for s in c.estacion_id]), axis=0, weights=w)
    return out


def cuantiles_dia(serie: np.ndarray, fechas: pd.DatetimeIndex) -> np.ndarray:
    """Cuantil de cada día dentro de la distribución del mismo mes en el período de referencia."""
    q = np.full(len(serie), np.nan)
    ref = (fechas.year >= REF[0]) & (fechas.year <= REF[1])
    for m in range(1, 13):
        im = fechas.month == m
        r = np.sort(serie[im & ref & ~np.isnan(serie)])
        if len(r) == 0:
            continue
        q[im] = (np.searchsorted(r, serie[im], side='left') + np.searchsorted(r, serie[im], side='right')) / 2 / len(r)
    return np.clip(q, Q[0], Q[-1])


def aplicar(serie, fechas, delta, q=None):
    q = cuantiles_dia(serie, fechas) if q is None else q
    out = serie.copy()
    for m in range(1, 13):
        im = fechas.month == m
        out[im] = serie[im] + np.interp(q[im], Q, delta[m - 1])
    return out


# ---------------------------------------------------------------- validación cruzada
def conteos(s: pd.Series, umbral, op) -> pd.Series:
    g = s.groupby(s.index.year)
    ok = g.count() >= 0.95 * g.size().clip(lower=365)
    c = g.apply(lambda x: op(x.dropna(), umbral).sum())
    return c[ok]


def validar(F, meta, dia, mod_est) -> pd.DataFrame:
    filas = []
    conv = set(s for (s, v, m) in F if v == 'suelo')
    for _, e in meta.iterrows():
        sid = e.estacion_id
        if sid not in mod_est:
            continue
        d = dia[dia.estacion_id == sid].set_index('fecha')
        mod = mod_est[sid]
        fechas = mod.index
        lejos = meta.estacion_id[dist_km(e.lat, e.lng, meta.lat.values, meta.lng.values) < EXCLUIR_KM].tolist()
        for v, metricas in (('tmin', [('lt0', 0, np.less), ('le3', 3, np.less_equal)]),
                            ('tmax', [('gt35', 35, np.greater)])):
            obs = d[v]
            preds = {'cruda': mod[v].values}
            for metodo in ('idw', 'cercana'):
                delta = trasladar(F, meta, e.lat, e.lng, v, metodo, excluir=lejos)
                if delta is not None:
                    preds[metodo] = aplicar(mod[v].values, fechas, delta)
            for nombre, umbral, op in metricas:
                co = conteos(obs, umbral, op)
                if len(co) < 5:
                    continue
                for metodo, p in preds.items():
                    cm = conteos(pd.Series(p, index=fechas), umbral, op).reindex(co.index)
                    ok = cm.notna()
                    filas.append(dict(estacion_id=sid, nombre=e.nombre, variable=v, metrica=nombre, metodo=metodo,
                                      n_anios=int(ok.sum()), obs_media=co[ok].mean(), mod_media=cm[ok].mean(),
                                      mae=(cm[ok] - co[ok]).abs().mean(), sesgo=(cm[ok] - co[ok]).mean()))
        # helada a 5 cm
        if sid in conv and 'tmin_suelo' in d:
            co = conteos(d.tmin_suelo, 0, np.less)
            if len(co) >= 5:
                dt = trasladar(F, meta, e.lat, e.lng, 'tmin', 'idw', excluir=lejos)
                ds = trasladar(F, meta[meta.estacion_id.isin(conv)], e.lat, e.lng, 'suelo', 'cercana', excluir=lejos)
                preds = {'cruda': mod.tmin.values}
                if dt is not None and ds is not None:
                    q = cuantiles_dia(mod.tmin.values, fechas)
                    abrigo = aplicar(mod.tmin.values, fechas, dt, q)
                    preds['abrigo_idw+suelo_cercana'] = aplicar(abrigo, fechas, ds, q)
                for metodo, p in preds.items():
                    cm = conteos(pd.Series(p, index=fechas), 0, np.less).reindex(co.index)
                    ok = cm.notna()
                    filas.append(dict(estacion_id=sid, nombre=e.nombre, variable='tmin_suelo', metrica='lt0_suelo',
                                      metodo=metodo, n_anios=int(ok.sum()), obs_media=co[ok].mean(),
                                      mod_media=cm[ok].mean(), mae=(cm[ok] - co[ok]).abs().mean(),
                                      sesgo=(cm[ok] - co[ok]).mean()))
    return pd.DataFrame(filas)


def decidir(val: pd.DataFrame) -> dict:
    dec = {}
    for v, mets in (('tmin', ['lt0', 'le3']), ('tmax', ['gt35']), ('tmin_suelo', ['lt0_suelo'])):
        s = val[(val.variable == v) & val.metrica.isin(mets)]
        if s.empty:
            dec[v] = {'adoptado': None, 'motivo': 'sin estaciones para validar'}
            continue
        # sólo estaciones con todos los métodos evaluados
        piv = s.groupby(['estacion_id', 'metodo']).mae.sum().unstack()
        piv = piv.dropna()
        medias = piv.mean().to_dict()
        base = medias['cruda']
        mejor = min((m for m in medias if m != 'cruda'), key=lambda m: medias[m], default=None)
        mejora = (base - medias[mejor]) / base if mejor and base > 0 else 0
        dec[v] = {'mae_medio': {k: round(x, 2) for k, x in medias.items()}, 'n_estaciones': int(len(piv)),
                  'mejor': mejor, 'mejora': round(mejora, 3),
                  'adoptado': mejor if mejora >= MEJORA else None,
                  'regla': f'adoptar el de menor MAE si mejora ≥ {MEJORA:.0%} sobre la cruda'}
    return dec


# ---------------------------------------------------------------- aplicación a todos los píxeles
def aplicar_todo(F, meta, dec) -> None:
    pix = pd.read_parquet(DERIV / 'pixel_era5.parquet')
    fechas = pd.DatetimeIndex(pd.read_parquet(DERIV / 'era5_fechas.parquet').fecha)
    tmin = np.load(DERIV / 'era5_tmin_diario.npy')
    tmax = np.load(DERIV / 'era5_tmax_diario.npy')
    conv = meta[meta.estacion_id.isin({s for (s, v, m) in F if v == 'suelo'})]
    suelo = np.full_like(tmin, np.nan) if dec['tmin_suelo']['adoptado'] else None
    for i, p in enumerate(pix.itertuples()):
        q = cuantiles_dia(tmin[:, i], fechas)
        if dec['tmin']['adoptado']:
            d = trasladar(F, meta, p.lat, p.lng, 'tmin', dec['tmin']['adoptado'])
            tmin[:, i] = aplicar(tmin[:, i], fechas, d, q)
        if suelo is not None:
            ds = trasladar(F, conv, p.lat, p.lng, 'suelo', 'cercana')
            suelo[:, i] = aplicar(tmin[:, i], fechas, ds, q)
        if dec['tmax']['adoptado']:
            d = trasladar(F, meta, p.lat, p.lng, 'tmax', dec['tmax']['adoptado'])
            tmax[:, i] = aplicar(tmax[:, i], fechas, d)
    log(f'corregidos {len(pix):,} píxeles')
    camp, est, est_anio = grupos(fechas)
    tm = (tmax + tmin) / 2
    m = {
        'gdd10': lambda i: np.nansum(np.maximum(tm[i] - 10, 0), axis=0),
        'dias_tmax35': lambda i: (tmax[i] > 35).sum(axis=0).astype(np.int16),
        'tmax_media': lambda i: np.nanmean(tmax[i], axis=0),
        'tmin_media': lambda i: np.nanmean(tmin[i], axis=0),
    }
    ids = pix.pixel_id.values
    c = agregar(fechas, [camp], ['campania'], m, ids, completo_por_calendario(['campania']))
    e = agregar(fechas, [est_anio, est], ['anio_estacion', 'estacion'], m, ids,
                completo_por_calendario(['anio_estacion', 'estacion']))
    doyv = fechas.dayofyear.values

    def primera(i):
        h = tmin[i] < 0
        return np.where(h.any(axis=0), doyv[i][h.argmax(axis=0)], -1).astype(np.int16)

    def ultima(i):
        h = tmin[i][::-1] < 0
        return np.where(h.any(axis=0), doyv[i][::-1][h.argmax(axis=0)], -1).astype(np.int16)

    mh = {
        'dias_tmin_lt0': lambda i: (tmin[i] < 0).sum(axis=0).astype(np.int16),
        'dias_tmin_lt_m2': lambda i: (tmin[i] < -2).sum(axis=0).astype(np.int16),
        'dias_tmin_le3': lambda i: (tmin[i] <= 3).sum(axis=0).astype(np.int16),
        'primera_doy': primera, 'ultima_doy': ultima,
        'tmin_abs': lambda i: np.nanmin(tmin[i], axis=0),
    }
    if suelo is not None:
        mh['dias_helada_suelo'] = lambda i: (suelo[i] < 0).sum(axis=0).astype(np.int16)
    h = agregar(fechas, [fechas.year.values], ['anio'], mh, ids, completo_por_calendario(['anio']))
    for col in ('primera_doy', 'ultima_doy'):
        h[col] = h[col].where(h[col] >= 0)
    if not dec['tmax']['adoptado']:
        for df in (c, e):
            df.drop(columns=['dias_tmax35', 'tmax_media'], inplace=True)
    if not dec['tmin']['adoptado']:
        h = h.iloc[0:0]
    c.to_parquet(DERIV / 'campania_era5_corr.parquet', index=False)
    e.to_parquet(DERIV / 'estacion_era5_corr.parquet', index=False)
    h.to_parquet(DERIV / 'helada_era5_corr.parquet', index=False)
    log(f'campañas {c.campania.nunique()}, años de helada {h.anio.nunique() if len(h) else 0}')


def main() -> None:
    t0 = time.time()
    meta, dia = estaciones()
    puntos = {r.estacion_id: (r.lat, r.lng) for r in meta.itertuples()}
    mod = series_modelo('era5', ['tmin', 'tmax'], puntos)
    mod = {k: v - 273.15 for k, v in mod.items()}
    log(f'{len(meta)} estaciones con temperatura; ERA5 en {len(mod)} de sus píxeles')
    F = ajustar(meta, dia, mod)
    filas = [dict(estacion_id=s, variable=v, mes=m, q=list(np.round(Q, 3)), delta=list(np.round(f, 3)))
             for (s, v, m), f in F.items()]
    pd.DataFrame(filas).to_parquet(DERIV / 'correccion_funciones.parquet', index=False)
    log(f'funciones: {len(F)} (estación × variable × mes)')
    val = validar(F, meta, dia, mod)
    val.to_parquet(DERIV / 'correccion_validacion.parquet', index=False)
    dec = decidir(val)
    (DERIV / 'correccion_decision.json').write_text(json.dumps(dec, indent=1, ensure_ascii=False), encoding='utf-8')
    for v, d in dec.items():
        log(f'{v}: {d}')
    aplicar_todo(F, meta, dec)
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
