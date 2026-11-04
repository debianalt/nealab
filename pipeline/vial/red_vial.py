"""Distancia POR LA RED DE CAMINOS de cada macizo forestal del NEA AR hasta el pavimento más cercano.

El camino más corto desde una plantación hasta una ruta pavimentada termina al tocar el pavimento: todo su largo
es camino de tierra, ripio o sin dato de superficie. Esa cifra («km de camino sin pavimentar hasta el asfalto»)
es la que pesa en el flete y la que se corta con lluvia; la línea recta (distancia_macizos.py) la subestima.

Grafo (sobre vial.caminos, importar_vial.py):
  - cada tramo se densifica a ≤ 50 m y cada par de vértices consecutivos es una arista (largo geodésico);
  - los nodos se identifican por coordenada redondeada a 1e-7°: en OSM los cruces comparten nodo y conectan;
  - conectores: los extremos sueltos (grado 1) se unen al vértice más cercano de OTRO tramo a ≤ 60 m si el tramo es
    de Microsoft (quedan a ~25 m de OSM por construcción) y a ≤ 30 m si es de OSM (cruces mal cerrados);
  - orígenes: todos los vértices de tramos con superficie = 'pavimento'. Un Dijkstra multi-origen (scipy) da a cada
    vértice los metros por camino hasta el pavimento más cercano.

Macizo → red: el vértice conectado más cercano al borde del macizo (de los 8 más cercanos por índice, el de menor
distancia geodésica); acceso_m es ese tramo recto y km_red_pav = acceso + red. No se elige el vértice que minimiza
acceso + red: eso premia cortar campo a través en línea recta (en Corrientes, 1 de cada 4 macizos «entraba» a más de
1 km). Si el camino más cercano es una isla sin conexión, el acceso llega hasta la red conectada y lo dice acceso_m.

Escribe en el Hexoma:
  vial.nodos     id, km_a_pav, geom                 (vértices del grafo; reutilizable)
  vial.aristas   id, source, target, cost, reverse_cost, largo_m, clase, superficie, camino_id
                 (formato pgRouting; la geometría sale de vial.nodos)
  vial.macizos   + acceso_m, km_red_pav

Uso (desde neahub/, después de importar_vial.py y distancia_macizos.py):
    python pipeline/vial/red_vial.py
"""
from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path

import numpy as np
import shapely
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

DENS_M = 50
TOL_MS, TOL_OSM = 60.0, 30.0
PUENTE_M = float(os.getenv("VIAL_PUENTE_M", 500))   # salto máximo isla → red conectada
RONDAS = 6
K_MACIZO = 8
R = 6371008.8


def metros(lon1, lat1, lon2, lat2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def copy_rows(cur, tabla: str, cols: list[str], filas) -> None:
    buf = io.StringIO()
    for f in filas:
        buf.write('\t'.join('' if v is None else str(v) for v in f) + '\n')
    buf.seek(0)
    cur.copy_expert(f"COPY {tabla} ({', '.join(cols)}) FROM STDIN WITH (FORMAT text, NULL '')", buf)


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    cur = conn.cursor()
    log('leyendo vial.caminos densificado…')
    cur.execute(f"""SELECT id, fuente, superficie = 'pavimento', clase, superficie,
                           ST_AsBinary(ST_Segmentize(geom::geography, {DENS_M})::geometry) FROM vial.caminos""")
    rows = cur.fetchall()
    cam_id = np.array([r[0] for r in rows])
    es_ms = np.array([r[1] == 'microsoft' for r in rows])
    es_pav = np.array([bool(r[2]) for r in rows])
    clase = [r[3] for r in rows]
    superf = [r[4] for r in rows]
    geoms = shapely.from_wkb([bytes(r[5]) for r in rows])
    del rows
    partes, idx_tramo = shapely.get_parts(geoms, return_index=True)
    coords, idx_parte = shapely.get_coordinates(partes, return_index=True)
    log(f'  {len(cam_id):,} tramos, {len(partes):,} partes, {len(coords):,} vértices ({time.time() - t0:.0f} s)')

    # Nodos por coordenada redondeada
    q = np.round(coords * 1e7).astype(np.int64)
    uniq, nodo = np.unique(q, axis=0, return_inverse=True)
    nodo = nodo.ravel()
    xy = uniq / 1e7
    n = len(uniq)
    # Aristas: vértices consecutivos de la misma parte
    mismo = idx_parte[1:] == idx_parte[:-1]
    a, b = nodo[:-1][mismo], nodo[1:][mismo]
    parte_e = idx_parte[:-1][mismo]
    tramo_e = idx_tramo[parte_e]
    largo = metros(xy[a, 0], xy[a, 1], xy[b, 0], xy[b, 1])
    ok = a != b
    a, b, largo, tramo_e = a[ok], b[ok], largo[ok], tramo_e[ok]
    log(f'  grafo: {n:,} nodos, {len(a):,} aristas ({time.time() - t0:.0f} s)')

    # Conectores desde extremos sueltos
    grado = np.bincount(np.concatenate([a, b]), minlength=n)
    ini = np.r_[0, np.flatnonzero(np.diff(idx_parte)) + 1]
    fin = np.r_[ini[1:] - 1, len(idx_parte) - 1]
    lat0 = np.radians(xy[:, 1].mean())
    pxy = np.c_[xy[:, 0] * np.cos(lat0), xy[:, 1]] * (np.pi / 180 * R)
    arbol = cKDTree(pxy)
    parte_de_nodo = np.full(n, -1)
    parte_de_nodo[nodo] = idx_parte   # una parte cualquiera por nodo (alcanza para descartar la propia)
    con_a, con_b, con_l = [], [], []
    for vtx in (ini, fin):
        extremos = nodo[vtx]
        partes_ext = idx_parte[vtx]
        suelto = grado[extremos] == 1
        tol = np.where(es_ms[idx_tramo[partes_ext]], TOL_MS, TOL_OSM)
        cand = np.flatnonzero(suelto)
        # Por radio, no por k vecinos: los tramos de Microsoft traen vértices cada pocos metros y los k más cercanos
        # eran todos del mismo tramo (con k=6 Corrientes quedaba con 1 de cada 3 macizos junto a una isla).
        vecinos = arbol.query_ball_point(pxy[extremos[cand]], r=TOL_MS)
        for c, vs in zip(cand, vecinos):
            e = extremos[c]
            vs = [k for k in vs if k != e and parte_de_nodo[k] != partes_ext[c]]
            if not vs:
                continue
            dd = np.hypot(*(pxy[vs] - pxy[e]).T)
            i = int(np.argmin(dd))
            if dd[i] <= tol[c]:
                con_a.append(e); con_b.append(vs[i]); con_l.append(float(dd[i]))
    con_a, con_b, con_l = map(np.array, (con_a, con_b, con_l))
    log(f'  conectores: {len(con_a):,} ({time.time() - t0:.0f} s)')

    A = np.concatenate([a, con_a]).astype(np.int64)
    B = np.concatenate([b, con_b]).astype(np.int64)
    L = np.concatenate([largo, con_l])
    G = coo_matrix((L, (A, B)), shape=(n, n)).tocsr()
    ncomp, comp = connected_components(G, directed=False)
    pav_nodos = np.unique(np.concatenate([a[es_pav[tramo_e]], b[es_pav[tramo_e]]]))
    log(f'  {ncomp:,} componentes; {len(pav_nodos):,} nodos sobre pavimento; Dijkstra multi-origen…')
    dist = dijkstra(G, directed=False, indices=pav_nodos, min_only=True)
    alcanzable = np.isfinite(dist)
    # Islas: grupos de caminos (casi siempre detecciones de Microsoft en Corrientes) sin ningún enlace detectado con la
    # red que llega al pavimento. Cada isla se une a la red por su par de nodos más cercano si el salto es ≤ PUENTE_M:
    # el camión recorre el camino de la isla y sólo el salto sin mapear se cuenta en línea recta.
    # En rondas: una isla unida en la ronda anterior pasa a ser red y puede recibir a la siguiente (detecciones
    # cortadas bajo el dosel encadenan varios pedazos).
    for ronda in range(1, RONDAS + 1):
        isla = ~alcanzable
        ids_ok = np.flatnonzero(alcanzable)
        d_isla, j_isla = cKDTree(pxy[ids_ok]).query(pxy[isla], distance_upper_bound=PUENTE_M)
        nodos_isla = np.flatnonzero(isla)
        comp_isla = comp[nodos_isla]
        orden = np.lexsort((d_isla, comp_isla))
        prim = orden[np.r_[True, comp_isla[orden][1:] != comp_isla[orden][:-1]]]   # el nodo más cercano de cada isla
        prim = prim[np.isfinite(d_isla[prim])]
        if not len(prim):
            break
        pa, pb, pl = nodos_isla[prim], ids_ok[j_isla[prim]], d_isla[prim]
        log(f'  ronda {ronda}: {len(pa):,} islas unidas a la red (≤ {PUENTE_M:.0f} m, mediana {np.median(pl):.0f} m)')
        A = np.concatenate([A, pa]); B = np.concatenate([B, pb]); L = np.concatenate([L, pl])
        con_a = np.concatenate([con_a, pa]); con_b = np.concatenate([con_b, pb]); con_l = np.concatenate([con_l, pl])
        G = coo_matrix((L, (A, B)), shape=(n, n)).tocsr()
        dist = dijkstra(G, directed=False, indices=pav_nodos, min_only=True)
        alcanzable = np.isfinite(dist)
    log(f'  {alcanzable.mean() * 100:.1f} % de los nodos llega al pavimento ({time.time() - t0:.0f} s)')

    # ── Postgres ─────────────────────────────────────────────────────────────────
    log('escribiendo vial.nodos y vial.aristas…')
    cur.execute("""
        DROP TABLE IF EXISTS vial.nodos, vial.aristas;
        CREATE TABLE vial.nodos (id bigint PRIMARY KEY, x double precision, y double precision, km_a_pav real);
        CREATE TABLE vial.aristas (id bigserial PRIMARY KEY, source bigint, target bigint, largo_m real,
                                   camino_id int, clase text, superficie text);
    """)
    km = np.where(alcanzable, dist / 1000, np.nan)
    copy_rows(cur, 'vial.nodos', ['id', 'x', 'y', 'km_a_pav'],
              ((i, f'{xy[i, 0]:.7f}', f'{xy[i, 1]:.7f}', None if np.isnan(km[i]) else f'{km[i]:.4f}') for i in range(n)))
    cid = np.concatenate([cam_id[tramo_e], np.full(len(con_a), -1)])
    cl = np.concatenate([np.array(clase, dtype=object)[tramo_e], np.full(len(con_a), 'conector', dtype=object)])
    su = np.concatenate([np.array(superf, dtype=object)[tramo_e], np.full(len(con_a), 'conector', dtype=object)])
    copy_rows(cur, 'vial.aristas', ['source', 'target', 'largo_m', 'camino_id', 'clase', 'superficie'],
              zip(A, B, (f'{v:.1f}' for v in L), cid, cl, su))
    cur.execute("""
        ALTER TABLE vial.nodos ADD COLUMN geom geometry(Point, 4326);
        UPDATE vial.nodos SET geom = ST_SetSRID(ST_MakePoint(x, y), 4326);
        ALTER TABLE vial.nodos DROP COLUMN x, DROP COLUMN y;
        CREATE INDEX nodos_geom_alc ON vial.nodos USING gist (geom) WHERE km_a_pav IS NOT NULL;
        ALTER TABLE vial.aristas ADD COLUMN cost double precision, ADD COLUMN reverse_cost double precision;
        UPDATE vial.aristas SET cost = largo_m, reverse_cost = largo_m;
        CREATE INDEX ON vial.aristas (source); CREATE INDEX ON vial.aristas (target);
        COMMENT ON TABLE vial.nodos IS 'Vértices del grafo vial NEA AR (vial.caminos densificado a 50 m); km_a_pav = km por la red hasta el pavimento más cercano (NULL = sin conexión). pipeline/vial/red_vial.py';
        COMMENT ON TABLE vial.aristas IS 'Aristas del grafo vial (formato pgRouting: source, target, cost, reverse_cost en m; no dirigido). clase/superficie = conector para los empalmes ≤60 m (Microsoft) o ≤30 m (OSM) y los puentes isla → red ≤500 m. pipeline/vial/red_vial.py';
    """)
    conn.commit()
    log(f'  listo ({time.time() - t0:.0f} s); macizo → red…')
    cur.execute(f"""
        ALTER TABLE vial.macizos DROP COLUMN IF EXISTS acceso_m, DROP COLUMN IF EXISTS km_red_pav;
        ALTER TABLE vial.macizos ADD COLUMN acceso_m real, ADD COLUMN km_red_pav real;
        UPDATE vial.macizos m SET acceso_m = s.acc, km_red_pav = s.km
        FROM (SELECT m2.fid, best.acc, best.km FROM vial.macizos m2
              CROSS JOIN LATERAL (
                  SELECT c.acc, c.acc / 1000 + c.km_a_pav AS km FROM (
                      SELECT ST_Distance(m2.geom::geography, nd.geom::geography) AS acc, nd.km_a_pav
                      FROM (SELECT geom, km_a_pav FROM vial.nodos WHERE km_a_pav IS NOT NULL
                            ORDER BY geom <-> m2.geom LIMIT {K_MACIZO}) nd) c
                  ORDER BY c.acc LIMIT 1) best) s
        WHERE m.fid = s.fid;
    """)
    conn.commit()
    log(f'  macizos listos ({time.time() - t0:.0f} s)')

    # ── chequeos ─────────────────────────────────────────────────────────────────
    cur.execute("""SELECT count(*), count(km_red_pav),
                          sum((km_red_pav * 1000 < dist_pav_m - acceso_m - 100)::int),
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY km_red_pav * 1000 / nullif(dist_pav_m, 0))
                          FILTER (WHERE dist_pav_m > 500),
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY acceso_m)
                   FROM vial.macizos""")
    tot, con, viol, rodeo, acc = cur.fetchone()
    log(f'  {con:,} de {tot:,} macizos conectados; red < recta − acceso − 100 m en {viol:,}; '
        f'rodeo mediano {rodeo:.2f}; acceso mediano {acc:.0f} m')
    assert viol <= 0.005 * tot, f'{viol} macizos con red más corta que la línea recta'
    cur.execute("""SELECT prov, round(percentile_cont(0.5) WITHIN GROUP (ORDER BY km_red_pav)::numeric, 1),
                          round(100 * sum(CASE WHEN km_red_pav IS NULL THEN superficie::numeric END)
                                / sum(superficie::numeric), 2)
                   FROM vial.macizos GROUP BY 1 ORDER BY 1""")
    for prov, med, sin in cur.fetchall():
        log(f'  {prov}: mediana {med} km por la red · {sin or 0} % de la superficie sin conexión')
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
