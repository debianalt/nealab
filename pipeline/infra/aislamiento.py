"""¿Qué población queda aislada si se corta cada cruce? Aristas de corte del grafo vial de Misiones.

Una arista de corte (en teoría de grafos, «puente») es la que, quitada, parte la red en dos. Si un cruce
camino × cauce cae sobre una arista de corte, cortarlo aísla todo lo que queda del otro lado; si no, existe un
desvío (cuánto más largo es asunto de la fase 2). Es un cálculo exacto sobre la red, no un modelo.

Pasos:
  1. Grafo: las aristas de `vial.aristas` cuyo origen cae en el recorte de Misiones (incluye los empalmes sin
     camino_id y un borde de Corrientes), sin dirección. Las aristas paralelas cuentan: dos caminos entre los
     mismos nodos no son corte.
  2. Aristas de corte: Tarjan iterativo (numba) sobre el grafo en CSR, en tiempo lineal.
  3. Bloques: componentes conexas sin las aristas de corte. Las aristas de corte unen bloques en un bosque;
     la raíz es el bloque que contiene el nodo más cercano a Posadas (plaza 9 de Julio). Los bloques que no
     llegan a la raíz son islas de la red (`desconectado`).
  4. Población 2022 por celda (`censo_2022.cruce`, est_personas) y hospitales + CAPS del visor al nodo más
     cercano; sumas por subárbol.
  5. Cada cruce de `infra.cruces` → la arista más cercana de su mismo camino. Si es de corte:
     personas_aisladas = población del subárbol del lado lejano a Posadas; hospitales/caps de ese lado.

Escribe:
  infra.aristas_corte  arista_id, bloque_lejano, personas, hospitales, caps
  infra.cruces         + arista_id, dist_arista_m, aisla, personas_aisladas, hospitales_aislados,
                         caps_aislados, desconectado

Uso (desde neahub/, después de cruces.py):
    python pipeline/infra/aislamiento.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from numba import njit
from pyproj import Transformer
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

POSADAS = (-55.8961, -27.3671)
A_UTM = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)


@njit(cache=True)
def aristas_de_corte(n, indptr, vecino, arista, n_aristas):
    disc = -np.ones(n, np.int64)
    low = np.zeros(n, np.int64)
    it = np.zeros(n, np.int64)
    pila_v = np.empty(n, np.int64)
    pila_e = np.empty(n, np.int64)
    corte = np.zeros(n_aristas, np.bool_)
    t = 0
    for s in range(n):
        if disc[s] != -1:
            continue
        top = 0
        pila_v[0] = s
        pila_e[0] = -1
        disc[s] = t
        low[s] = t
        t += 1
        it[s] = indptr[s]
        while top >= 0:
            v = pila_v[top]
            if it[v] < indptr[v + 1]:
                k = it[v]
                it[v] += 1
                e = arista[k]
                if e == pila_e[top]:
                    continue
                w = vecino[k]
                if disc[w] == -1:
                    disc[w] = t
                    low[w] = t
                    t += 1
                    top += 1
                    pila_v[top] = w
                    pila_e[top] = e
                    it[w] = indptr[w]
                elif disc[w] < low[v]:
                    low[v] = disc[w]
            else:
                e_v = pila_e[top]
                top -= 1
                if top >= 0:
                    u = pila_v[top]
                    if low[v] < low[u]:
                        low[u] = low[v]
                    if low[v] > disc[u]:
                        corte[e_v] = True
    return corte


def cargar_grafo(conn):
    x0, y0, x1, y1 = drenaje.BBOX
    q = f"""SELECT a.id, a.source, a.target, a.camino_id, ST_X(ns.geom) x1, ST_Y(ns.geom) y1,
                   ST_X(nt.geom) x2, ST_Y(nt.geom) y2
            FROM vial.aristas a JOIN vial.nodos ns ON ns.id = a.source JOIN vial.nodos nt ON nt.id = a.target
            WHERE ns.geom && ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, 4326)"""
    buf = io.StringIO()
    conn.cursor().copy_expert(f'COPY ({q}) TO STDOUT WITH CSV', buf)
    buf.seek(0)
    return pd.read_csv(buf, header=None, names=['id', 'source', 'target', 'camino_id', 'x1', 'y1', 'x2', 'y2'],
                       dtype={'camino_id': 'float64'})


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    a = cargar_grafo(conn)
    a = a[a.source != a.target].reset_index(drop=True)
    log(f'grafo: {len(a):,} aristas ({time.time() - t0:.0f} s)')

    # nodos compactos
    ids, inv = np.unique(np.concatenate([a.source.values, a.target.values]), return_inverse=True)
    u, v = inv[:len(a)], inv[len(a):]
    n, m = len(ids), len(a)
    nx_ = np.empty(n)
    ny_ = np.empty(n)
    nx_[u], ny_[u] = a.x1.values, a.y1.values
    nx_[v], ny_[v] = a.x2.values, a.y2.values

    # CSR sin dirección; cada arista aparece dos veces con su índice
    orig = np.concatenate([u, v])
    dest = np.concatenate([v, u])
    eid = np.concatenate([np.arange(m), np.arange(m)])
    orden = np.argsort(orig, kind='stable')
    orig, dest, eid = orig[orden], dest[orden], eid[orden]
    indptr = np.zeros(n + 1, np.int64)
    np.add.at(indptr, orig + 1, 1)
    indptr = np.cumsum(indptr)
    corte = aristas_de_corte(n, indptr, dest.astype(np.int64), eid.astype(np.int64), m)
    log(f'aristas de corte: {corte.sum():,} de {m:,} ({time.time() - t0:.0f} s)')

    # bloques sin las aristas de corte
    keep = ~corte
    g = coo_matrix((np.ones(keep.sum()), (u[keep], v[keep])), shape=(n, n))
    nb, bloque = connected_components(g, directed=False)
    nx_utm, ny_utm = A_UTM.transform(nx_, ny_)
    arbol = cKDTree(np.column_stack([nx_utm, ny_utm]))
    px, py = A_UTM.transform(*POSADAS)
    raiz = bloque[arbol.query([px, py])[1]]

    # bosque de bloques: BFS desde la raíz
    bu, bv = bloque[u[corte]], bloque[v[corte]]
    ec = np.flatnonzero(corte)
    ady = pd.DataFrame({'a': np.concatenate([bu, bv]), 'b': np.concatenate([bv, bu]), 'e': np.concatenate([ec, ec])})
    ady = ady.groupby('a')[['b', 'e']].apply(lambda d: list(zip(d.b, d.e))).to_dict()
    padre = -np.ones(nb, np.int64)
    padre[raiz] = raiz
    orden_bfs = [raiz]
    i = 0
    while i < len(orden_bfs):
        b = orden_bfs[i]
        i += 1
        for c, _ in ady.get(b, []):
            if padre[c] == -1:
                padre[c] = b
                orden_bfs.append(c)
    conectado = padre >= 0
    log(f'bloques: {nb:,}; conectados a Posadas: {conectado.sum():,} ({time.time() - t0:.0f} s)')

    # población y salud → nodo → bloque
    pob = pd.read_sql("""SELECT h3::text h3, sum(est_personas) p,
                                (h3_cell_to_lat_lng(h3))[0] lon, (h3_cell_to_lat_lng(h3))[1] lat
                         FROM censo_2022.cruce WHERE territorio_id = 1 GROUP BY h3""", conn)
    qx, qy = A_UTM.transform(pob.lon.values, pob.lat.values)
    _, nodo_pob = arbol.query(np.column_stack([qx, qy]))
    salud = pd.read_sql("""SELECT 'hospital' tipo, ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hospitales
                           UNION ALL SELECT 'caps', ST_X(geom), ST_Y(geom) FROM infra.visor_caps""", conn)
    sx, sy = A_UTM.transform(salud.lon.values, salud.lat.values)
    _, nodo_sal = arbol.query(np.column_stack([sx, sy]))
    p_bloque = np.bincount(bloque[nodo_pob], weights=pob.p.values, minlength=nb)
    h_bloque = np.bincount(bloque[nodo_sal[salud.tipo.values == 'hospital']], minlength=nb).astype(float)
    c_bloque = np.bincount(bloque[nodo_sal[salud.tipo.values == 'caps']], minlength=nb).astype(float)
    log(f'población asignada: {pob.p.sum():,.0f} personas; en bloques conectados: '
        f'{p_bloque[conectado].sum():,.0f}')

    # sumas por subárbol (orden BFS inverso)
    sub_p, sub_h, sub_c = p_bloque.copy(), h_bloque.copy(), c_bloque.copy()
    for b in reversed(orden_bfs[1:]):
        pb = padre[b]
        sub_p[pb] += sub_p[b]
        sub_h[pb] += sub_h[b]
        sub_c[pb] += sub_c[b]

    # cada arista de corte: el bloque lejano es el hijo
    lejano = np.where(padre[bu] == bv, bu, bv)
    lejano = np.where(conectado[bu] | conectado[bv], lejano, -1)
    ac = pd.DataFrame({'arista_id': a.id.values[ec], 'bloque_lejano': lejano})
    okl = ac.bloque_lejano >= 0
    ac['personas'] = np.where(okl, sub_p[ac.bloque_lejano.clip(0)], np.nan)
    ac['hospitales'] = np.where(okl, sub_h[ac.bloque_lejano.clip(0)], np.nan)
    ac['caps'] = np.where(okl, sub_c[ac.bloque_lejano.clip(0)], np.nan)
    ac[['hospitales', 'caps']] = ac[['hospitales', 'caps']].round().astype('Int64')

    # cruces → arista más cercana del mismo camino
    cr = pd.read_sql('SELECT id, camino_id, ST_X(geom) lon, ST_Y(geom) lat FROM infra.cruces', conn)
    mx, my = A_UTM.transform((a.x1.values + a.x2.values) / 2, (a.y1.values + a.y2.values) / 2)
    x1u, y1u = A_UTM.transform(a.x1.values, a.y1.values)
    x2u, y2u = A_UTM.transform(a.x2.values, a.y2.values)
    cx, cy = A_UTM.transform(cr.lon.values, cr.lat.values)
    k = 24
    _, cand = cKDTree(np.column_stack([mx, my])).query(np.column_stack([cx, cy]), k=k)
    ax, ay, bx, by = x1u[cand], y1u[cand], x2u[cand], y2u[cand]
    dx, dy = bx - ax, by - ay
    tt = np.clip(((cx[:, None] - ax) * dx + (cy[:, None] - ay) * dy) / np.maximum(dx * dx + dy * dy, 1e-9), 0, 1)
    dist = np.hypot(ax + tt * dx - cx[:, None], ay + tt * dy - cy[:, None])
    mismo = a.camino_id.values[cand] == cr.camino_id.values[:, None]
    dist_m = np.where(mismo, dist, dist + 1e6)  # preferir el mismo camino; si no aparece, la más cercana
    j = np.argmin(dist_m, axis=1)
    elegido = cand[np.arange(len(cr)), j]
    cr['arista_idx'] = elegido
    cr['arista_id'] = a.id.values[elegido]
    cr['dist_arista_m'] = dist[np.arange(len(cr)), j]
    cr['aisla'] = corte[elegido]
    b_arista = bloque[u[elegido]]
    cr['desconectado'] = ~conectado[b_arista]
    ac_idx = ac.set_index('arista_id')
    cr = cr.join(ac_idx[['personas', 'hospitales', 'caps']], on='arista_id')
    cr.loc[~cr.aisla, ['personas', 'hospitales', 'caps']] = np.nan
    cr[['hospitales', 'caps']] = cr[['hospitales', 'caps']].astype('Int64')
    log(f'cruces: {len(cr):,}; sobre arista de corte: {cr.aisla.sum():,}; en islas: {cr.desconectado.sum():,}; '
        f'distancia a su arista p50 {cr.dist_arista_m.median():.1f} m, p99 {cr.dist_arista_m.quantile(.99):.0f} m')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.aristas_corte')
    cur.execute('CREATE TABLE infra.aristas_corte (arista_id bigint PRIMARY KEY, bloque_lejano integer, '
                'personas real, hospitales smallint, caps smallint)')
    buf = io.StringIO()
    ac.to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY infra.aristas_corte FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("COMMENT ON TABLE infra.aristas_corte IS 'Aristas de corte de vial.aristas en el recorte de Misiones "
                "(pipeline/infra/aislamiento.py): población 2022, hospitales y CAPS del lado lejano a Posadas; "
                "bloque_lejano = -1 si ninguno de los dos lados llega a Posadas.'")
    for col, tipo in [('arista_id', 'bigint'), ('dist_arista_m', 'real'), ('aisla', 'boolean'),
                      ('personas_aisladas', 'real'), ('hospitales_aislados', 'smallint'),
                      ('caps_aislados', 'smallint'), ('desconectado', 'boolean')]:
        cur.execute(f'ALTER TABLE infra.cruces DROP COLUMN IF EXISTS {col}')
        cur.execute(f'ALTER TABLE infra.cruces ADD COLUMN {col} {tipo}')
    cur.execute('CREATE TEMP TABLE _a (id int, arista_id bigint, dist real, aisla boolean, p real, h smallint, '
                'c smallint, desc_ boolean)')
    buf = io.StringIO()
    cr[['id', 'arista_id', 'dist_arista_m', 'aisla', 'personas', 'hospitales', 'caps', 'desconectado']].to_csv(
        buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY _a FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""UPDATE infra.cruces c SET arista_id = _a.arista_id, dist_arista_m = _a.dist, aisla = _a.aisla,
                   personas_aisladas = _a.p, hospitales_aislados = _a.h, caps_aislados = _a.c, desconectado = _a.desc_
                   FROM _a WHERE _a.id = c.id""")
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
