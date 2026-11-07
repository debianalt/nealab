"""Río Uruguay contra los puentes de la RP2: niveles diarios medidos, cero de escala y cortes por puente.

Todo lo que entra es medición, salvo dos supuestos declarados:
  1. entre dos hidrómetros, la superficie del río varía linealmente con el kilómetro de río;
  2. en crecida el Uruguay remansa por el arroyo: el nivel en el puente ≈ el del río en la desembocadura (no se
     modela la crecida propia del arroyo por lluvia en su cuenca).

Fuentes:
  - lecturas diarias de la escala de Prefectura, API pública del INA (alerta.ina.gob.ar/a5, red «escalas Prefectura
    Nacional»): Pepirí Miní 60, El Soberbio 61, Alba Posse 63, Panambí 64, San Javier 65, Puerto Concepción 66,
    Garruchos 67. Lectura MANUAL una vez por día: subestima el pico del día (18-oct-2023 en El Soberbio: lectura
    15,8 m, pico 17,95 m según la prensa).
  - cota de seguridad de 25 puentes RP2 (infra.visor_puentes_rp2, visor de emergencias de Misiones).
  - cero de escala en msnm, dos estimaciones independientes:
      (a) tablas «altura → msnm» de los simuladores del Monitor provincial (sig.misiones.gob.ar/mapas/monitor/<pueblo>/)
      (b) FABDEM implícito: superficie del río en FABDEM junto a la estación (mínimo a ≤ 300 m del punto proyectado
          sobre el eje) − mediana de la escala en 2011–2015 (adquisición de Copernicus GLO-30). Método de la demo
          El Soberbio, donde dio 118,9 contra 118 del Monitor.
    Regla fijada antes de mirar (plan del 7-nov-2026): si |a − b| ≤ 1,5 m se usa (a) con ±1 m; si no, (b) y se reporta.
    Resultado (7-nov-2026): coinciden en El Soberbio (118 / 118,9); en Alba Posse, Panambí y San Javier difieren 3–11 m
    y se usa (b). Coherencia de datum: las cotas de los puentes coinciden con FABDEM en el camino junto al puente (±1–3 m)
    y el cero del Monitor de San Javier pondría el agua por encima del río 5 km aguas arriba. La variante con ceros del
    Monitor se calcula igual (columnas mon_*) para mostrar cuánto depende el resultado.
  - cada día, cada puente usa la estación más cercana CON lectura aguas arriba y aguas abajo (≤ 200 km entre ellas):
    las series de Pepirí Miní y Panambí tienen huecos; el error de esa interpolación se mide en validar_rio.py.
  - eje del Uruguay: infra.osm_cauces (OSM), tramos encadenados desde aguas arriba; kilómetro de río = distancia
    sobre esa cadena del punto proyectado.

Escribe en el Hexoma:
  infra.rio_estaciones       serie, nombre, km_rio, cero (a), cero (b), cero usado, incertidumbre, alerta/evacuación
  infra.rio_lecturas         serie, fecha, altura_m (máx. del día), qc
  infra.rio_puente_diario    puente, fecha, nivel_msnm, margen_m (nivel − cota), cortado
  infra.rio_puente_resumen   por puente: cobertura, años con corte, probabilidad anual empírica, días por año,
                             GEV y Gumbel sobre máximos anuales, sensibilidad +1 y +2 m

Uso (desde neahub/, después de importar_visor.py, cauces_osm.py y drenaje.py):
    python pipeline/infra/rio_uruguay.py [--rebajar]
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import shapely
from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

INA = 'https://alerta.ina.gob.ar/a5/obs/puntual/observaciones'
CRUDO = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'ina'
UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar)'}
A_UTM = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)
A_GEO = Transformer.from_crs('EPSG:32721', 'EPSG:4326', always_xy=True)

# serie INA → (nombre, cero del Monitor provincial en msnm o None, alerta, evacuación del Monitor o None)
ESTACIONES = {
    60: ('Pepirí Miní', None, None, None),
    61: ('El Soberbio', 118.0, 11.0, 13.0),
    63: ('Alba Posse', 107.0, 8.0, 12.0),
    64: ('Panambí', 96.0, 7.0, 9.0),
    65: ('San Javier', 85.0, 8.0, 10.0),
    66: ('Puerto Concepción', None, 16.5, 17.5),
    67: ('Garruchos', None, None, None),
}
REGLA_CERO_M = 1.5
PERFIL = CRUDO.parent / 'perfil_uruguay_fabdem.csv'
INCERT_MONITOR_M = 1.0
REF_FABDEM = ('2011-01-01', '2015-12-31')
SALTO_M = 10.0
MESES_MIN, DIAS_MES_MIN = 9, 8   # un año cuenta si ≥ 9 meses tienen ≥ 8 días con nivel en el puente


def bajar_serie(sid: int, rebajar: bool) -> pd.DataFrame:
    p = CRUDO / f'ina_{sid}.json'
    if rebajar or not p.exists():
        r = requests.get(INA, params={'series_id': sid, 'timestart': '1950-01-01', 'timeend': '2030-01-01',
                                      'format': 'json'}, headers=UA, timeout=300)
        r.raise_for_status()
        CRUDO.mkdir(parents=True, exist_ok=True)
        p.write_bytes(r.content)
    d = json.loads(p.read_text(encoding='utf-8'))
    d = d if isinstance(d, list) else d.get('rows', d)
    df = pd.DataFrame({'fecha': pd.to_datetime([x['timestart'][:10] for x in d]),
                       'altura_m': pd.to_numeric([x['valor'] for x in d], errors='coerce')})
    df['serie'] = sid
    return df


def qc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.groupby(['serie', 'fecha'], as_index=False).altura_m.max().sort_values(['serie', 'fecha'])
    df['qc'] = 'ok'
    df.loc[(df.altura_m > 60) | (df.altura_m < -3) | df.altura_m.isna(), 'qc'] = 'fuera_de_rango'
    ok = df.qc == 'ok'
    for sid, g in df[ok].groupby('serie'):
        prev = g.altura_m.shift(1)
        nxt = g.altura_m.shift(-1)
        # pico de un día: se aparta de los dos vecinos en el MISMO sentido y por más de SALTO_M. Una subida real
        # (3 → 12 → 21 → 27 → 21,5 m, El Soberbio, julio de 1983) no es un pico: el río sube 6–9 m por día en el cañón.
        d1, d2 = g.altura_m - prev, g.altura_m - nxt
        pico = (np.sign(d1) == np.sign(d2)) & (d1.abs() > SALTO_M) & (d2.abs() > SALTO_M)
        df.loc[g.index[pico.values], 'qc'] = 'salto_aislado'
    return df


def cadena_uruguay(conn) -> shapely.LineString:
    """Tramos OSM del Uruguay encadenados desde aguas arriba (el extremo más al este), en UTM 21S."""
    d = pd.read_sql("""SELECT ST_AsBinary((ST_Dump(ST_LineMerge(ST_Union(geom)))).geom) wkb
                       FROM infra.osm_cauces WHERE waterway = 'river' AND nombre LIKE '%Uruguay'""", conn)
    partes = [shapely.transform(shapely.from_wkb(bytes(w)), lambda xy: np.column_stack(A_UTM.transform(xy[:, 0], xy[:, 1])))
              for w in d.wkb]
    partes = [p for p in partes if p.length > 2000]
    inicio = max(partes, key=lambda p: max(p.coords[0][0], p.coords[-1][0]))
    if inicio.coords[0][0] < inicio.coords[-1][0]:
        inicio = shapely.LineString(inicio.coords[::-1])
    coords = list(inicio.coords)
    resto = [p for p in partes if p is not inicio]
    cubierto = inicio.buffer(500)
    while resto:
        fin = shapely.Point(coords[-1])
        cand = []
        for p in resto:
            for rev in (False, True):
                c = p.coords[::-1] if rev else p.coords
                cand.append((fin.distance(shapely.Point(c[0])), p, list(c)))
        dist, p, c = min(cand, key=lambda x: x[0])
        resto.remove(p)
        if p.difference(cubierto).length < 0.3 * p.length or dist > 15000:
            continue  # brazo lateral ya cubierto, o pedazo lejano
        coords += c
        cubierto = cubierto.union(p.buffer(500))
    return shapely.LineString(coords)


def proyectar(eje: shapely.LineString, lon, lat) -> tuple[np.ndarray, np.ndarray]:
    x, y = A_UTM.transform(np.asarray(lon, float), np.asarray(lat, float))
    pts = shapely.points(x, y)
    km = shapely.line_locate_point(eje, pts) / 1000
    dist = shapely.distance(eje, pts)
    return km, dist


def cero_fabdem(eje, km: float, lect: pd.DataFrame, sid: int) -> tuple[float, float, float]:
    import rasterio
    p = eje.interpolate(km * 1000)
    lon, lat = A_GEO.transform(p.x, p.y)
    with rasterio.open(drenaje.FUENTES / 'fabdem_misiones.tif') as src:
        f, c = src.index(lon, lat)
        w = src.read(1, window=((f - 10, f + 11), (c - 10, c + 11)))  # ±10 celdas ≈ ±300 m
    zmin = float(np.nanmin(w))
    g = lect[(lect.serie == sid) & (lect.qc == 'ok') & (lect.fecha >= REF_FABDEM[0]) & (lect.fecha <= REF_FABDEM[1])]
    med = float(g.altura_m.median()) if len(g) else np.nan
    return zmin - med, zmin, med


CERCA_KM = 5.0
MAX_TRAMO_KM = 250.0   # dos estaciones a más de esto no se interpolan (El Soberbio–San Javier = 201 km)


def niveles_en_puentes(lect: pd.DataFrame, ceros: pd.Series, kms: pd.Series, pu: pd.DataFrame) -> pd.DataFrame:
    """Nivel diario del río en cada puente: interpolación por kilómetro de río entre la estación más cercana CON
    lectura ese día aguas arriba y la más cercana con lectura aguas abajo (≤ MAX_TRAMO_KM entre ellas)."""
    ok = lect[lect.qc == 'ok'].copy()
    ok['nivel_msnm'] = ok.altura_m + ok.serie.map(ceros)
    tabla = ok.pivot_table(index='fecha', columns='serie', values='nivel_msnm')
    tabla = tabla.reindex(pd.date_range(tabla.index.min(), tabla.index.max(), freq='D'))
    out = []
    for p in pu.itertuples():
        arriba = kms[kms <= p.km_rio].sort_values(ascending=False)
        abajo = kms[kms > p.km_rio].sort_values()
        if arriba.empty or abajo.empty:
            continue
        nivel = pd.Series(np.nan, index=tabla.index)
        s_ar = pd.Series(np.nan, index=tabla.index)
        s_ab = pd.Series(np.nan, index=tabla.index)
        for s1 in arriba.index:            # prioridad: la pareja más cercana primero
            for s2 in abajo.index:
                if kms[s2] - kms[s1] > MAX_TRAMO_KM:
                    continue
                f = (p.km_rio - kms[s1]) / (kms[s2] - kms[s1])
                v = tabla[s1] * (1 - f) + tabla[s2] * f
                m = nivel.isna() & v.notna()
                nivel[m], s_ar[m], s_ab[m] = v[m], s1, s2
        # respaldo: sin pareja ese día, pero un hidrómetro a ≤ CERCA_KM de río → su nivel (pendiente ~0,2 m/km)
        cerca = (kms - p.km_rio).abs()
        for sc in cerca[cerca <= CERCA_KM].sort_values().index:
            m = nivel.isna() & tabla[sc].notna()
            nivel[m], s_ar[m], s_ab[m] = tabla[sc][m], sc, sc
        d = pd.DataFrame({'gid': p.gid, 'fecha': tabla.index, 'nivel_msnm': nivel.values,
                          'serie_arriba': s_ar.values, 'serie_abajo': s_ab.values})
        d = d[d.nivel_msnm.notna()].copy()
        d['serie_arriba'] = d.serie_arriba.astype(int)
        d['serie_abajo'] = d.serie_abajo.astype(int)
        d['tramo_km'] = d.serie_abajo.map(kms).values - d.serie_arriba.map(kms).values
        d['margen_m'] = d.nivel_msnm - p.cota
        d['cortado'] = d.margen_m >= 0
        out.append(d)
    return pd.concat(out, ignore_index=True)


def resumir(diario: pd.DataFrame, pu: pd.DataFrame, ajustes: bool = True) -> pd.DataFrame:
    from scipy import stats
    rng = np.random.default_rng(7)
    res = []
    for p in pu.itertuples():
        d = diario[diario.gid == p.gid]
        if d.empty:
            continue
        d = d.assign(anio=d.fecha.dt.year)
        por_mes = d.groupby(['anio', d.fecha.dt.month]).size()
        meses_ok = (por_mes >= DIAS_MES_MIN).groupby(level=0).sum()
        anios = meses_ok.index[meses_ok >= MESES_MIN]
        da = d[d.anio.isin(anios)]
        maxa = da.groupby('anio').nivel_msnm.max()
        cort = da.groupby('anio').cortado.sum()
        n = len(anios)
        fila = dict(gid=p.gid, nombre=p.nombre, cota=p.cota, km_rio=p.km_rio, dist_eje_m=p.dist_eje_m,
                    anios_con_datos=n, primer_anio=int(min(anios)) if n else None,
                    ultimo_anio=int(max(anios)) if n else None, tramo_km_mediano=float(d.tramo_km.median()),
                    anios_con_corte=int((cort > 0).sum()), p_anual_empirica=(cort > 0).mean() if n else np.nan,
                    dias_cortado_media=cort.mean() if n else np.nan, dias_cortado_max=int(cort.max()) if n else 0,
                    primer_corte=d[d.cortado].fecha.min(), ultimo_corte=d[d.cortado].fecha.max(),
                    nivel_max_msnm=maxa.max() if n else np.nan, margen_max_m=maxa.max() - p.cota if n else np.nan,
                    anios_con_corte_mas1=int((maxa + 1 >= p.cota).sum()),
                    anios_con_corte_mas2=int((maxa + 2 >= p.cota).sum()),
                    t_retorno_gev=np.nan, t_retorno_gev_p10=np.nan, t_retorno_gev_p90=np.nan, t_retorno_gumbel=np.nan)
        if ajustes and n >= 20:
            x = maxa.values
            c, loc, sc = stats.genextreme.fit(x)
            fila['t_retorno_gev'] = 1 / max(stats.genextreme.sf(p.cota, c, loc, sc), 1e-6)
            lg, sg = stats.gumbel_r.fit(x)
            fila['t_retorno_gumbel'] = 1 / max(stats.gumbel_r.sf(p.cota, lg, sg), 1e-6)
            boot = []
            for _ in range(300):
                xb = rng.choice(x, len(x), replace=True)
                try:
                    cb, lb, sb = stats.genextreme.fit(xb)
                    boot.append(1 / max(stats.genextreme.sf(p.cota, cb, lb, sb), 1e-6))
                except Exception:  # noqa: BLE001
                    pass
            if boot:
                fila['t_retorno_gev_p10'], fila['t_retorno_gev_p90'] = np.percentile(boot, [10, 90])
        res.append(fila)
    return pd.DataFrame(res)


PASO_PERFIL_M = 100.0
MEDIA_SECCION_M = 300.0


def perfil_fabdem(eje: shapely.LineString) -> pd.DataFrame:
    """Superficie del agua del Uruguay en FABDEM a lo largo del eje: cada PASO_PERFIL_M, el punto más bajo de una
    sección transversal de ±MEDIA_SECCION_M (las orillas son más altas); mediana por kilómetro; y regresión isotónica
    decreciente (el agua sólo baja río abajo). Lee FABDEM punto por punto, sin cargar el ráster entero."""
    import rasterio
    from scipy.optimize import isotonic_regression
    s = np.arange(0, eje.length, PASO_PERFIL_M)
    p0 = shapely.line_interpolate_point(eje, s)
    p1 = shapely.line_interpolate_point(eje, np.minimum(s + 50, eje.length))
    tx, ty = shapely.get_x(p1) - shapely.get_x(p0), shapely.get_y(p1) - shapely.get_y(p0)
    nrm = np.hypot(tx, ty)
    nx, ny = -ty / np.maximum(nrm, 1e-9), tx / np.maximum(nrm, 1e-9)
    off = np.arange(-MEDIA_SECCION_M, MEDIA_SECCION_M + 1, 30.0)
    X = shapely.get_x(p0)[:, None] + nx[:, None] * off[None, :]
    Y = shapely.get_y(p0)[:, None] + ny[:, None] * off[None, :]
    lon, lat = A_GEO.transform(X.ravel(), Y.ravel())
    with rasterio.open(drenaje.FUENTES / 'fabdem_misiones.tif') as src:
        z = np.array([v[0] for v in src.sample(zip(lon, lat))], dtype=float).reshape(X.shape)
    zmin = z.min(axis=1)
    d = pd.DataFrame({'km': s / 1000, 'zmin': zmin})
    d['km_bin'] = np.floor(d.km).astype(int)
    b = d.groupby('km_bin').zmin.median().reset_index()
    iso = isotonic_regression(b.zmin.values, increasing=False).x
    b['z_iso'] = iso
    return b


def cero_perfil(perfil: pd.DataFrame, km: float, lect: pd.DataFrame, sid: int) -> float:
    z = float(np.interp(km, perfil.km_bin.values + 0.5, perfil.z_iso.values))
    g = lect[(lect.serie == sid) & (lect.qc == 'ok') & (lect.fecha >= REF_FABDEM[0]) & (lect.fecha <= REF_FABDEM[1])]
    return z - float(g.altura_m.median()) if len(g) else np.nan


def error_interpolacion(lect: pd.DataFrame, ceros: pd.Series, kms: pd.Series, alertas: pd.Series) -> pd.Series:
    """|error| mediano (m) al predecir cada estación intermedia con las vecinas con lectura ese día, en días con la
    escala ≥ alerta (o ≥ P90 si no hay alerta). Criterio para elegir el juego de ceros."""
    ok = lect[lect.qc == 'ok'].copy()
    ok['msnm'] = ok.altura_m + ok.serie.map(ceros)
    tab = ok.pivot_table(index='fecha', columns='serie', values='msnm')
    out = {}
    for t in [61, 63, 64, 65]:
        k = kms[t]
        otros = kms.drop(t)
        pred = pd.Series(np.nan, index=tab.index)
        for s1 in otros[otros < k].sort_values(ascending=False).index:
            for s2 in otros[otros > k].sort_values().index:
                span = kms[s2] - kms[s1]
                if span > MAX_TRAMO_KM:
                    continue
                f = (k - kms[s1]) / span
                v = tab[s1] * (1 - f) + tab[s2] * f
                m = pred.isna() & v.notna()
                pred[m] = v[m]
        h = tab[t] - ceros[t]
        um = alertas[t] if pd.notna(alertas[t]) else h.quantile(.9)
        m = tab[t].notna() & pred.notna() & (h >= um)
        out[t] = float((pred - tab[t])[m].abs().median())
    return pd.Series(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    args = ap.parse_args()
    t0 = time.time()
    conn = connect(DB)
    lect = qc(pd.concat([bajar_serie(s, args.rebajar) for s in ESTACIONES], ignore_index=True))
    for sid, g in lect.groupby('serie'):
        ok = g[g.qc == 'ok']
        log(f'  serie {sid} {ESTACIONES[sid][0]}: {len(ok):,} lecturas válidas {ok.fecha.min():%Y-%m-%d} → '
            f'{ok.fecha.max():%Y-%m-%d}; qc: {g.qc.value_counts().drop("ok", errors="ignore").to_dict()}')

    eje = cadena_uruguay(conn)
    log(f'eje del Uruguay encadenado: {eje.length / 1000:,.0f} km')
    est = pd.read_sql('SELECT series_id, ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hidrometros '
                      f'WHERE series_id IN ({",".join(map(str, ESTACIONES))})', conn)
    est['km_rio'], est['dist_eje_m'] = proyectar(eje, est.lon, est.lat)
    filas = []
    for r in est.itertuples():
        nombre, c_mon, alerta, evac = ESTACIONES[r.series_id]
        c_fab, zmin, med = cero_fabdem(eje, r.km_rio, lect, r.series_id)
        if c_mon is not None and abs(c_mon - c_fab) <= REGLA_CERO_M:
            usado, inc, fuente = c_mon, INCERT_MONITOR_M, 'monitor'
        elif c_mon is not None:
            usado, inc, fuente = c_fab, abs(c_mon - c_fab), 'fabdem (difiere del monitor)'
        else:
            usado, inc, fuente = c_fab, np.nan, 'fabdem (sin monitor)'
        filas.append((r.series_id, nombre, r.lon, r.lat, r.km_rio, r.dist_eje_m, c_mon, c_fab, zmin, med, usado, inc,
                      fuente, alerta, evac))
    est = pd.DataFrame(filas, columns=['serie', 'nombre', 'lon', 'lat', 'km_rio', 'dist_eje_m', 'cero_monitor',
                                       'cero_fabdem', 'fabdem_rio_msnm', 'mediana_2011_2015', 'cero_msnm',
                                       'incertidumbre_m', 'fuente_cero', 'alerta_m', 'evacuacion_m']).sort_values('km_rio')
    # (c) perfil isotónico de FABDEM. Regla fijada el 7-nov-2026 ANTES de mirar: gana el juego de ceros con menor
    # |error| medio de interpolación en crecida (4 estaciones intermedias), siempre que el perfil dé El Soberbio a
    # ≤ 1,5 m de 118 (confirmado por Monitor, FABDEM puntual y la nota del 28-jun-2014); si no, queda la regla (a)/(b).
    perfil = perfil_fabdem(eje)
    est['cero_perfil'] = [cero_perfil(perfil, r.km_rio, lect, r.serie) for r in est.itertuples()]
    kms_e, alert = est.set_index('serie').km_rio, est.set_index('serie').alerta_m
    err_ab = error_interpolacion(lect, est.set_index('serie').cero_msnm, kms_e, alert)
    err_c = error_interpolacion(lect, est.set_index('serie').cero_perfil, kms_e, alert)
    control = abs(est.set_index('serie').cero_perfil[61] - 118.0)
    log(f'error de interpolación en crecida: regla a/b {err_ab.round(2).to_dict()} (media {err_ab.mean():.2f}); '
        f'perfil {err_c.round(2).to_dict()} (media {err_c.mean():.2f}); control El Soberbio |perfil − 118| = {control:.2f} m')
    if control <= REGLA_CERO_M and err_c.mean() < err_ab.mean():
        est['cero_msnm'] = est.cero_perfil
        est['fuente_cero'] = 'perfil isotónico FABDEM'
        est['incertidumbre_m'] = err_c.reindex(est.serie).values
        log('  → se usa el perfil')
    else:
        log('  → queda la regla (a)/(b)')
    PERFIL.parent.mkdir(parents=True, exist_ok=True)
    perfil.to_csv(PERFIL, index=False)
    log('estaciones:\n' + est[['serie', 'nombre', 'km_rio', 'dist_eje_m', 'cero_monitor', 'cero_fabdem', 'cero_perfil',
                               'cero_msnm', 'fuente_cero']].round(1).to_string(index=False))

    pu = pd.read_sql('SELECT gid, nombre, cota_seguridad_msnm cota, ST_X(geom) lon, ST_Y(geom) lat '
                     'FROM infra.visor_puentes_rp2 WHERE cota_seguridad_msnm IS NOT NULL', conn)
    pu['km_rio'], pu['dist_eje_m'] = proyectar(eje, pu.lon, pu.lat)
    kms = est.set_index('serie').km_rio
    ceros = est.set_index('serie').cero_msnm
    diario = niveles_en_puentes(lect, ceros, kms, pu)
    res = resumir(diario, pu)
    # variante: ceros del Monitor donde existen (para mostrar cuánto depende el resultado del cero)
    ceros_mon = est.set_index('serie').apply(lambda r: r.cero_monitor if pd.notna(r.cero_monitor) else r.cero_msnm, axis=1)
    res_mon = resumir(niveles_en_puentes(lect, ceros_mon, kms, pu), pu, ajustes=False)
    res = res.merge(res_mon[['gid', 'anios_con_datos', 'anios_con_corte', 'p_anual_empirica']].rename(columns={
        'anios_con_datos': 'mon_anios_con_datos', 'anios_con_corte': 'mon_anios_con_corte',
        'p_anual_empirica': 'mon_p_anual_empirica'}), on='gid', how='left')
    log('puentes:\n' + res[['nombre', 'cota', 'anios_con_datos', 'anios_con_corte', 'p_anual_empirica',
                            'dias_cortado_media', 'margen_max_m', 't_retorno_gumbel', 'mon_anios_con_corte']]
        .round(2).to_string(index=False, max_rows=100))

    cur = conn.cursor()

    def copiar(df: pd.DataFrame, tabla_: str, ddl: str, comentario: str) -> None:
        cur.execute(f'DROP TABLE IF EXISTS {tabla_} CASCADE')
        cur.execute(f'CREATE TABLE {tabla_} ({ddl})')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N', date_format='%Y-%m-%d')
        buf.seek(0)
        cur.copy_expert(f"COPY {tabla_} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
        cur.execute(f'COMMENT ON TABLE {tabla_} IS %s', (comentario,))

    copiar(est, 'infra.rio_estaciones',
           'serie int PRIMARY KEY, nombre text, lon double precision, lat double precision, km_rio real, dist_eje_m real, '
           'cero_monitor real, cero_fabdem real, fabdem_rio_msnm real, mediana_2011_2015 real, cero_msnm real, '
           'incertidumbre_m real, fuente_cero text, alerta_m real, evacuacion_m real, cero_perfil real',
           'Hidrómetros del Uruguay (escala de Prefectura, INA) con kilómetro de río y cero de escala en msnm: Monitor '
           'provincial y FABDEM implícito (rio_uruguay.py)')
    copiar(lect[['serie', 'fecha', 'altura_m', 'qc']], 'infra.rio_lecturas',
           'serie int, fecha date, altura_m real, qc text',
           'Lecturas diarias de la escala de Prefectura (API INA a5), máximo del día; qc marca sin borrar')
    copiar(diario[['gid', 'fecha', 'nivel_msnm', 'margen_m', 'cortado', 'serie_arriba', 'serie_abajo', 'tramo_km']],
           'infra.rio_puente_diario',
           'gid int, fecha date, nivel_msnm real, margen_m real, cortado boolean, serie_arriba int, serie_abajo int, tramo_km real',
           'Nivel del Uruguay interpolado por kilómetro de río en cada puente RP2 con cota, día por día (rio_uruguay.py)')
    cur.execute('CREATE INDEX ON infra.rio_puente_diario (gid, fecha)')
    cols = list(res.columns)
    tipos = {c: 'text' if c in ('nombre',) else 'date' if c in ('primer_corte', 'ultimo_corte') else 'real' for c in cols}
    res['primer_corte'] = pd.to_datetime(res.primer_corte).dt.strftime('%Y-%m-%d')
    res['ultimo_corte'] = pd.to_datetime(res.ultimo_corte).dt.strftime('%Y-%m-%d')
    tipos['gid'] = 'int PRIMARY KEY'
    copiar(res, 'infra.rio_puente_resumen', ', '.join(f'{c} {tipos[c]}' for c in cols),
           'Por puente RP2: años con datos (≥ 9 meses con ≥ 8 días), años con corte, probabilidad anual empírica, días cortado, '
           'margen máximo, períodos de retorno GEV/Gumbel de la cota y sensibilidad +1/+2 m (rio_uruguay.py)')
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
