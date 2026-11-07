"""Día de lluvia en Misiones: superficie, pendiente y velocidad de cada arista; minutos a hospital y CAPS por celda.

Con lluvia, los caminos de tierra colorada se vuelven lentos, poco transitables o peligrosos (sobre todo en
pendiente). Este script diferencia la superficie y calcula el acceso a salud en cuatro escenarios.

Atributos por arista (`infra.aristas_atributos`):
  - superficie efectiva en dos cotas, porque 5.357 km de Misiones no tienen superficie conocida:
      optimista  desconocida = pavimento en urbana/primaria/secundaria/terciaria, ripio en el resto
      pesimista  desconocida = tierra
  - pendiente_pct: FABDEM 30 m muestreado cada 25 m a lo largo de cada camino, pendiente sobre 150 m centrados (sobre
    50 m el ruido vertical de FABDEM pesa demasiado); a cada arista, la máxima de su tramo.
Velocidades (km/h): tabla VEL, SUPUESTOS explícitos, no mediciones. Por eso cada tiempo va acompañado de una medida
sin supuestos: los km de tierra del camino.

Escenarios:
  seco           todas las aristas, velocidad de seco
  lluvia         todas, velocidad de lluvia
  lluvia_fuerte  sin la tierra con pendiente ≥ PEND_PELIGRO; el resto a velocidad de lluvia
  todo_tiempo    sólo pavimento y ripio (¿quién tiene un camino que no depende del clima?)

Acceso (`infra.acceso_lluvia`, una fila por celda poblada × escenario × cota): Dijkstra multi-origen desde el nodo
más cercano a cada hospital (min_hospital) y a cada hospital o CAPS (min_salud); la celda entra por el nodo más
cercano a su centro (acceso_m). km_tierra_hospital = metros de tierra del camino más corto al hospital en ESE
escenario. sin_ruta = no hay camino al hospital en ese escenario.

Uso (desde neahub/, después de la fase 1: importar_visor.py, drenaje.py):
    python pipeline/infra/red_lluvia.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from numba import njit
from pyproj import Transformer
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402  (limpia PROJ_LIB antes de rasterio)
from build_hexoma import DB, connect, log  # noqa: E402

A_UTM = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)
PEND_PELIGRO = 10.0      # % — umbral de partida para tierra con lluvia fuerte; a contrastar con la DPV
PASO_PEND = 25.0         # m entre muestras de elevación
VENTANA_PEND = 150.0     # m sobre los que se mide la pendiente
UMBRALES_SENS = [6.0, 8.0, 10.0, 12.0, 15.0, 20.0]
ESCENARIOS = ['seco', 'lluvia', 'lluvia_fuerte', 'todo_tiempo']
COTAS = ['optimista', 'pesimista']

# velocidades en km/h: (seco, lluvia). SUPUESTOS; cambiar acá y nada más.
PAV = {'troncal': 80, 'primaria': 70, 'secundaria': 60, 'terciaria': 50, 'vecinal': 40, 'urbana': 30,
       'servicio': 20, 'huella': 30, 'detectado por imagen': 30, 'conector': 20}
TIERRA = {'troncal': (40, 20), 'primaria': (40, 20), 'secundaria': (40, 20), 'terciaria': (40, 20),
          'vecinal': (35, 15), 'urbana': (25, 12), 'huella': (20, 8), 'servicio': (15, 8),
          'detectado por imagen': (20, 8), 'conector': (20, 8)}
RIPIO = (40, 30)
LLUVIA_PAV = 0.85
PAV_SI_DESCONOCIDA = {'urbana', 'primaria', 'secundaria', 'terciaria', 'troncal'}


def velocidades(clase: np.ndarray, sup: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    seco = np.empty(len(clase))
    lluvia = np.empty(len(clase))
    for c in np.unique(clase):
        m = clase == c
        p = PAV.get(c, 30)
        t = TIERRA.get(c, (20, 8))
        mp, mr, mt = m & (sup == 'pavimento'), m & (sup == 'ripio/consolidado'), m & (sup == 'tierra')
        seco[mp], lluvia[mp] = p, p * LLUVIA_PAV
        seco[mr], lluvia[mr] = RIPIO
        seco[mt], lluvia[mt] = t
    return seco, lluvia


def superficie(clase: np.ndarray, sup: np.ndarray, cota: str) -> np.ndarray:
    s = sup.copy()
    desc = np.isin(s, ['desconocida', 'conector'])
    if cota == 'pesimista':
        s[desc] = 'tierra'
    else:
        s[desc & np.isin(clase, list(PAV_SI_DESCONOCIDA))] = 'pavimento'
        s[desc & ~np.isin(clase, list(PAV_SI_DESCONOCIDA))] = 'ripio/consolidado'
    # el conector hereda la peor superficie razonable en la pesimista y ripio en la optimista: es un empalme corto
    return s


def cargar_aristas(conn) -> pd.DataFrame:
    x0, y0, x1, y1 = drenaje.BBOX
    q = f"""SELECT a.id, a.source, a.target, a.camino_id, a.largo_m, a.clase, a.superficie,
                   ST_X(ns.geom) x1, ST_Y(ns.geom) y1, ST_X(nt.geom) x2, ST_Y(nt.geom) y2
            FROM vial.aristas a JOIN vial.nodos ns ON ns.id = a.source JOIN vial.nodos nt ON nt.id = a.target
            WHERE ns.geom && ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, 4326) AND a.source <> a.target"""
    buf = io.StringIO()
    conn.cursor().copy_expert(f'COPY ({q}) TO STDOUT WITH CSV', buf)
    buf.seek(0)
    return pd.read_csv(buf, header=None, names=['id', 'source', 'target', 'camino_id', 'largo_m', 'clase', 'superficie',
                                               'x1', 'y1', 'x2', 'y2'], dtype={'camino_id': 'float64'})


def pendientes(conn, a: pd.DataFrame) -> np.ndarray:
    import rasterio
    with rasterio.open(drenaje.FUENTES / 'fabdem_misiones.tif') as src:
        dem = src.read(1).astype(np.float32)
        tr = src.transform
    x0, y0, x1, y1 = drenaje.BBOX
    cam = pd.read_sql(f"""SELECT id, ST_AsBinary(geom) wkb FROM vial.caminos
                          WHERE geom && ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, 4326)""", conn)
    geo = shapely.line_merge(shapely.from_wkb(cam.wkb.map(bytes).values))
    utm = shapely.transform(geo, lambda xy: np.column_stack(A_UTM.transform(xy[:, 0], xy[:, 1])))
    largo = shapely.length(utm)
    n = np.maximum(np.ceil(largo / PASO_PEND).astype(int), 1) + 1
    ini = np.cumsum(n) - n
    idx = np.repeat(np.arange(len(utm)), n)
    off = np.arange(n.sum()) - np.repeat(ini, n)
    dist = np.minimum(off * PASO_PEND, np.repeat(largo, n))
    pts = shapely.line_interpolate_point(utm[idx], dist)
    to_geo = Transformer.from_crs('EPSG:32721', 'EPSG:4326', always_xy=True)
    lon, lat = to_geo.transform(shapely.get_x(pts), shapely.get_y(pts))
    # bilineal sobre FABDEM
    fc = (np.asarray(lon) - tr.c) / tr.a - 0.5
    ff = (np.asarray(lat) - tr.f) / tr.e - 0.5
    c0 = np.clip(np.floor(fc).astype(np.int64), 0, dem.shape[1] - 2)
    f0 = np.clip(np.floor(ff).astype(np.int64), 0, dem.shape[0] - 2)
    dc, df = np.clip(fc - c0, 0, 1), np.clip(ff - f0, 0, 1)
    z = (dem[f0, c0] * (1 - dc) * (1 - df) + dem[f0, c0 + 1] * dc * (1 - df)
         + dem[f0 + 1, c0] * (1 - dc) * df + dem[f0 + 1, c0 + 1] * dc * df)
    del dem
    # pendiente centrada sobre VENTANA_PEND (en las puntas, la ventana que entre, mínimo 50 m)
    h = int(VENTANA_PEND / PASO_PEND / 2)
    j = np.arange(len(z))
    fin = ini + n - 1
    lo = np.maximum(j - h, np.repeat(ini, n))
    hi = np.minimum(j + h, np.repeat(fin, n))
    span = (hi - lo) * PASO_PEND
    grad = np.where(span >= 50, np.abs(z[hi] - z[lo]) / np.maximum(span, 1) * 100, 0.0)
    # cada arista: máxima pendiente de las muestras entre sus dos extremos sobre su camino
    pos = {cid: k for k, cid in enumerate(cam.id.values)}
    k = np.array([pos.get(int(c), -1) if np.isfinite(c) else -1 for c in a.camino_id.values])
    ok = k >= 0
    ax, ay = A_UTM.transform(a.x1.values[ok], a.y1.values[ok])
    bx, by = A_UTM.transform(a.x2.values[ok], a.y2.values[ok])
    s1 = shapely.line_locate_point(utm[k[ok]], shapely.points(ax, ay))
    s2 = shapely.line_locate_point(utm[k[ok]], shapely.points(bx, by))
    i1 = ini[k[ok]] + np.clip(np.floor(np.minimum(s1, s2) / PASO_PEND).astype(int), 0, n[k[ok]] - 1)
    i2 = ini[k[ok]] + np.clip(np.ceil(np.maximum(s1, s2) / PASO_PEND).astype(int), 0, n[k[ok]] - 1)
    out = np.zeros(len(a))
    out[ok] = _max_rango(grad, i1, i2)
    return out


@njit(cache=True)
def _max_rango(v, i1, i2):
    r = np.empty(len(i1))
    for t in range(len(i1)):
        m = 0.0
        for q in range(i1[t], i2[t] + 1):
            if v[q] > m:
                m = v[q]
        r[t] = m
    return r


def grafo(u, v, w, keep, n):
    """CSR simétrico con el MÍNIMO peso entre aristas paralelas (csr_matrix sumaría los duplicados)."""
    d = pd.DataFrame({'a': np.minimum(u, v)[keep], 'b': np.maximum(u, v)[keep], 'w': w[keep]})
    d = d.groupby(['a', 'b'], sort=False).w.min().reset_index()
    a_, b_, w_ = d.a.values, d.b.values, np.maximum(d.w.values, 1e-3)
    return csr_matrix((np.concatenate([w_, w_]), (np.concatenate([a_, b_]), np.concatenate([b_, a_]))), shape=(n, n))


@njit(cache=True)
def tierra_por_arbol(orden, pred, indptr, indices, tierra_m):
    acc = np.zeros(len(pred))
    for node in orden:
        p = pred[node]
        if p < 0:
            continue
        for k in range(indptr[p], indptr[p + 1]):
            if indices[k] == node:
                acc[node] = acc[p] + tierra_m[k]
                break
    return acc


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    a = cargar_aristas(conn)
    log(f'aristas: {len(a):,} ({time.time() - t0:.0f} s)')
    a['pendiente_pct'] = pendientes(conn, a)
    log(f'pendientes ({time.time() - t0:.0f} s): p50 {np.median(a.pendiente_pct):.1f} %, '
        f'≥ {PEND_PELIGRO:.0f} %: {(a.pendiente_pct >= PEND_PELIGRO).mean():.1%} de las aristas')
    clase, sup0 = a.clase.values.astype(str), a.superficie.values.astype(str)

    ids, inv = np.unique(np.concatenate([a.source.values, a.target.values]), return_inverse=True)
    u, v = inv[:len(a)], inv[len(a):]
    n = len(ids)
    nx_, ny_ = np.empty(n), np.empty(n)
    nx_[u], ny_[u] = a.x1.values, a.y1.values
    nx_[v], ny_[v] = a.x2.values, a.y2.values
    ux, uy = A_UTM.transform(nx_, ny_)
    arbol = cKDTree(np.column_stack([ux, uy]))

    pob = pd.read_sql("""SELECT h3::text h3, sum(est_personas) p,
                                (h3_cell_to_lat_lng(h3))[0] lon, (h3_cell_to_lat_lng(h3))[1] lat
                         FROM censo_2022.cruce WHERE territorio_id = 1 GROUP BY h3 HAVING sum(est_personas) > 0""", conn)
    px, py = A_UTM.transform(pob.lon.values, pob.lat.values)
    acc_m, nodo_pob = arbol.query(np.column_stack([px, py]))
    sal = pd.read_sql("""SELECT 'hospital' tipo, ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hospitales
                         UNION ALL SELECT 'caps', ST_X(geom), ST_Y(geom) FROM infra.visor_caps""", conn)
    sx, sy = A_UTM.transform(sal.lon.values, sal.lat.values)
    _, nodo_sal = arbol.query(np.column_stack([sx, sy]))
    o_hosp = np.unique(nodo_sal[sal.tipo.values == 'hospital'])
    o_salud = np.unique(nodo_sal)

    filas, attrs = [], {'arista_id': a.id.values, 'pendiente_pct': a.pendiente_pct.values.astype(np.float32)}
    for cota in COTAS:
        sup = superficie(clase, sup0, cota)
        attrs[f'superficie_{cota}'] = sup
        vs, vl = velocidades(clase, sup)
        tierra = sup == 'tierra'
        largo = a.largo_m.values
        t_seco, t_lluvia = largo / (vs / 3.6), largo / (vl / 3.6)
        peligro = tierra & (a.pendiente_pct.values >= PEND_PELIGRO)
        todo_t = np.isin(sup, ['pavimento', 'ripio/consolidado'])
        esc = {'seco': (t_seco, np.ones(len(a), bool)), 'lluvia': (t_lluvia, np.ones(len(a), bool)),
               'lluvia_fuerte': (t_lluvia, ~peligro), 'todo_tiempo': (t_lluvia, todo_t)}
        for nombre, (w, keep) in esc.items():
            g = grafo(u, v, w, keep, n)
            d_h, pred, _ = dijkstra(g, directed=False, indices=o_hosp, min_only=True, return_predecessors=True)
            d_s = dijkstra(g, directed=False, indices=o_salud, min_only=True)
            # metros de tierra sobre el camino más corto al hospital (árbol de predecesores)
            gt = grafo(u, v, np.where(tierra, largo, 0.0) + 1e-6, keep, n)
            gt = gt.tocsr()
            # gt tiene la misma estructura que g (mismos pares); el peso es el de la arista de menor TIEMPO sólo si
            # coinciden; para la medida alcanza con la tierra de la arista paralela de menor tierra
            orden = np.argsort(d_h)
            orden = orden[np.isfinite(d_h[orden])]
            km_t = tierra_por_arbol(orden, pred, gt.indptr, gt.indices, gt.data) / 1000
            mh, ms = d_h[nodo_pob] / 60, d_s[nodo_pob] / 60
            filas.append(pd.DataFrame({
                'h3': pob.h3.values, 'escenario': nombre, 'cota': cota, 'personas': pob.p.values,
                'acceso_m': acc_m.astype(np.float32), 'min_hospital': np.where(np.isfinite(mh), mh, np.nan),
                'min_salud': np.where(np.isfinite(ms), ms, np.nan),
                'km_tierra_hospital': np.where(np.isfinite(mh), km_t[nodo_pob], np.nan),
                'sin_ruta': ~np.isfinite(mh)}))
            f = filas[-1]
            w_ = f.personas.values
            okr = ~f.sin_ruta.values
            med = np.nan
            if okr.any():
                o = np.argsort(f.min_hospital.values[okr])
                cw = np.cumsum(w_[okr][o])
                med = f.min_hospital.values[okr][o][np.searchsorted(cw, cw[-1] / 2)]
            log(f'  {cota:9s} {nombre:13s} mediana {med:5.1f} min al hospital; sin ruta {w_[~okr].sum():,.0f} personas; '
                f'> 60 min {w_[okr & (f.min_hospital.values > 60)].sum():,.0f} ({time.time() - t0:.0f} s)')

    # sensibilidad al umbral de pendiente (lluvia_fuerte, ambas cotas): el umbral es un supuesto
    sens, sens_celda = [], []
    for cota in COTAS:
        sup = superficie(clase, sup0, cota)
        _, vl = velocidades(clase, sup)
        t_l = a.largo_m.values / (vl / 3.6)
        for umbral in UMBRALES_SENS:
            keep = ~((sup == 'tierra') & (a.pendiente_pct.values >= umbral))
            d_h = dijkstra(grafo(u, v, t_l, keep, n), directed=False, indices=o_hosp, min_only=True)[nodo_pob] / 60
            sin = ~np.isfinite(d_h)
            sens.append((cota, umbral, float(pob.p.values[sin].sum()), float(pob.p.values[~sin & (d_h > 60)].sum())))
            sens_celda.append(pd.DataFrame({'h3': pob.h3.values[sin], 'cota': cota, 'umbral_pct': umbral}))
    sens = pd.DataFrame(sens, columns=['cota', 'umbral_pct', 'sin_ruta', 'mas_60_min'])
    log('sensibilidad al umbral:\n' + sens.round(0).to_string(index=False))

    acc = pd.concat(filas, ignore_index=True)
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.acceso_lluvia CASCADE')
    cur.execute("""CREATE TABLE infra.acceso_lluvia (h3 h3index, escenario text, cota text, personas real, acceso_m real,
                   min_hospital real, min_salud real, km_tierra_hospital real, sin_ruta boolean)""")
    buf = io.StringIO()
    acc.to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY infra.acceso_lluvia FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('CREATE INDEX ON infra.acceso_lluvia (escenario, cota)')
    cur.execute('CREATE INDEX ON infra.acceso_lluvia (h3)')
    cur.execute(f"""COMMENT ON TABLE infra.acceso_lluvia IS 'Minutos al hospital y a hospital o CAPS por celda poblada de
        Misiones en 4 escenarios de lluvia y 2 cotas de superficie desconocida (pipeline/infra/red_lluvia.py). Velocidades
        supuestas (tabla VEL del script); km_tierra_hospital no depende de ellas. Tierra peligrosa = pendiente ≥ {PEND_PELIGRO:.0f} %.'""")
    cur.execute('DROP TABLE IF EXISTS infra.sensibilidad_pendiente')
    cur.execute('CREATE TABLE infra.sensibilidad_pendiente (cota text, umbral_pct real, sin_ruta real, mas_60_min real)')
    buf = io.StringIO()
    sens.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY infra.sensibilidad_pendiente FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute('DROP TABLE IF EXISTS infra.sensibilidad_sin_ruta')
    cur.execute('CREATE TABLE infra.sensibilidad_sin_ruta (h3 h3index, cota text, umbral_pct real)')
    buf = io.StringIO()
    pd.concat(sens_celda).to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY infra.sensibilidad_sin_ruta FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute("COMMENT ON TABLE infra.sensibilidad_sin_ruta IS 'Celdas sin ruta al hospital con lluvia fuerte para "
                "cada umbral de pendiente (red_lluvia.py)'")
    cur.execute("COMMENT ON TABLE infra.sensibilidad_pendiente IS 'Personas sin ruta al hospital y a más de 60 min con "
                "lluvia fuerte según el umbral de pendiente de la tierra intransitable (red_lluvia.py)'")
    cur.execute('DROP TABLE IF EXISTS infra.aristas_atributos')
    cur.execute('CREATE TABLE infra.aristas_atributos (arista_id bigint PRIMARY KEY, pendiente_pct real, '
                'superficie_optimista text, superficie_pesimista text)')
    buf = io.StringIO()
    pd.DataFrame(attrs).to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY infra.aristas_atributos FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute("""COMMENT ON TABLE infra.aristas_atributos IS 'Pendiente (FABDEM, 150 m centrados, máxima del tramo) y
        superficie efectiva en dos cotas de cada arista de vial.aristas en el recorte de Misiones (red_lluvia.py)'""")
    cur.execute("""CREATE VIEW infra.v_acceso_municipio AS
        WITH c AS (SELECT a.*, m.nombre AS municipio FROM infra.acceso_lluvia a
                   JOIN infra.visor_municipios m
                     ON ST_Intersects(m.geom, ST_SetSRID(h3_cell_to_lat_lng(a.h3)::geometry, 4326)))
        SELECT municipio, escenario, cota, round(sum(personas)) personas,
               round(sum(personas) FILTER (WHERE sin_ruta)) sin_ruta,
               round(sum(personas) FILTER (WHERE min_hospital > 60)) mas_60_min,
               round((sum(personas * min_hospital) FILTER (WHERE NOT sin_ruta)
                      / nullif(sum(personas) FILTER (WHERE NOT sin_ruta), 0))::numeric, 1) min_hospital_medio,
               round((sum(personas * km_tierra_hospital) FILTER (WHERE NOT sin_ruta)
                      / nullif(sum(personas) FILTER (WHERE NOT sin_ruta), 0))::numeric, 1) km_tierra_medio
        FROM c GROUP BY 1, 2, 3""")
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
