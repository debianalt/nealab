"""¿Cuánto importa cada cruce? Dependientes, desvío y aislamiento con lluvia fuerte → columnas de `infra.cruces`.

Para cada cruce de `infra.cruces` (su arista en `vial.aristas`), con la superficie de la cota PESIMISTA de
red_lluvia.py (desconocida = tierra):
  dependientes_hospital  población cuyo camino más corto al hospital en SECO pasa por la arista (árbol de
                         predecesores del Dijkstra multi-origen desde los hospitales; se suma por subárbol)
  desvio_min_seco        minutos extra si la arista se corta: camino más corto u→v sin ESA arista (Dijkstra local
  desvio_min_lluvia      en numba, corte a CORTE_MIN) menos el tiempo de la arista; NULL si no hay alternativa en
                         CORTE_MIN (si aisla, no la hay nunca). Sólo para cruces con dependientes > 0.
  impacto_min_persona    dependientes × desvío con lluvia: el ranking de qué cruces pesan más
  personas_aisladas_lluvia_fuerte  aristas de corte (Tarjan de aislamiento.py) del grafo de lluvia_fuerte (sin la
                         tierra con pendiente ≥ PEND_PELIGRO): población del lado lejano a Posadas
  salud_aislada_lluvia_fuerte  hospitales + CAPS de ese lado (0 = el lado aislado no tiene salud propia)
  intransitable_lluvia_fuerte  la propia arista del cruce es tierra en pendiente ≥ PEND_PELIGRO

Uso (desde neahub/, después de red_lluvia.py):
    python pipeline/infra/desvios.py
"""
from __future__ import annotations

import heapq
import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from numba import njit
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components, dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from aislamiento import POSADAS, aristas_de_corte  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402
from red_lluvia import A_UTM, PEND_PELIGRO, cargar_aristas, grafo, superficie, velocidades  # noqa: E402

CORTE_MIN = 180.0
COTA = 'pesimista'


@njit(cache=True)
def desvios(indptr, vecino, peso, arista, qs, qt, qe, corte):
    n = len(indptr) - 1
    dist = np.full(n, np.inf)
    tocados = np.empty(n, np.int64)
    res = np.full(len(qs), np.inf)
    for q in range(len(qs)):
        s, t, ex = qs[q], qt[q], qe[q]
        nt = 0
        dist[s] = 0.0
        tocados[nt] = s
        nt += 1
        h = [(0.0, s)]
        while len(h) > 0:
            d, x = heapq.heappop(h)
            if d > dist[x]:
                continue
            if x == t:
                res[q] = d
                break
            if d > corte:
                break
            for k in range(indptr[x], indptr[x + 1]):
                if arista[k] == ex:
                    continue
                y = vecino[k]
                nd = d + peso[k]
                if nd < dist[y]:
                    if dist[y] == np.inf:
                        tocados[nt] = y
                        nt += 1
                    dist[y] = nd
                    heapq.heappush(h, (nd, y))
        for i in range(nt):
            dist[tocados[i]] = np.inf
    return res


def csr_con_ids(u, v, w, n):
    orig = np.concatenate([u, v])
    dest = np.concatenate([v, u])
    eid = np.concatenate([np.arange(len(u)), np.arange(len(u))])
    ww = np.concatenate([w, w])
    o = np.argsort(orig, kind='stable')
    indptr = np.zeros(n + 1, np.int64)
    np.add.at(indptr, orig[o] + 1, 1)
    return np.cumsum(indptr), dest[o].astype(np.int64), ww[o], eid[o].astype(np.int64)


def aislados(u, v, keep, n, pob_nodo, raiz_nodo, salud_nodo) -> tuple[np.ndarray, np.ndarray]:
    """Población y establecimientos de salud (hospital o CAPS) del lado lejano a Posadas de cada arista de corte del
    subgrafo `keep` (NaN si no es de corte). Un lado aislado CON salud propia no es lo mismo que uno sin ella."""
    ku, kv = u[keep], v[keep]
    m = len(ku)
    indptr, vec, _, eid = csr_con_ids(ku, kv, np.ones(m), n)
    corte = aristas_de_corte(n, indptr, vec, eid, m)
    g = coo_matrix((np.ones((~corte).sum()), (ku[~corte], kv[~corte])), shape=(n, n))
    nb, bloque = connected_components(g, directed=False)
    raiz = bloque[raiz_nodo]
    bu, bv = bloque[ku[corte]], bloque[kv[corte]]
    hijos = {}
    for a_, b_ in zip(np.concatenate([bu, bv]), np.concatenate([bv, bu])):
        hijos.setdefault(a_, []).append(b_)
    padre = -np.ones(nb, np.int64)
    padre[raiz] = raiz
    orden = [raiz]
    i = 0
    while i < len(orden):
        b = orden[i]
        i += 1
        for c in hijos.get(b, []):
            if padre[c] == -1:
                padre[c] = b
                orden.append(c)
    sub = np.bincount(bloque, weights=pob_nodo, minlength=nb)
    sal = np.bincount(bloque, weights=salud_nodo, minlength=nb)
    for b in reversed(orden[1:]):
        sub[padre[b]] += sub[b]
        sal[padre[b]] += sal[b]
    lej = np.where(padre[bu] == bv, bu, bv)
    ok = (padre[bu] >= 0) | (padre[bv] >= 0)
    res = np.full(len(u), np.nan)
    res_s = np.full(len(u), np.nan)
    idx_keep = np.flatnonzero(keep)[corte]
    res[idx_keep] = np.where(ok, sub[lej], np.nan)
    res_s[idx_keep] = np.where(ok, sal[lej], np.nan)
    return res, res_s


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    a = cargar_aristas(conn)
    at = pd.read_sql('SELECT arista_id, pendiente_pct FROM infra.aristas_atributos', conn)
    a = a.merge(at, left_on='id', right_on='arista_id', how='left')
    a['pendiente_pct'] = a.pendiente_pct.fillna(0)
    clase, sup = a.clase.values.astype(str), superficie(a.clase.values.astype(str), a.superficie.values.astype(str), COTA)
    vs, vl = velocidades(clase, sup)
    t_seco, t_lluvia = a.largo_m.values / (vs / 3.6), a.largo_m.values / (vl / 3.6)
    peligro = (sup == 'tierra') & (a.pendiente_pct.values >= PEND_PELIGRO)

    ids, inv = np.unique(np.concatenate([a.source.values, a.target.values]), return_inverse=True)
    u, v = inv[:len(a)], inv[len(a):]
    n = len(ids)
    nx_, ny_ = np.empty(n), np.empty(n)
    nx_[u], ny_[u] = a.x1.values, a.y1.values
    nx_[v], ny_[v] = a.x2.values, a.y2.values
    ux, uy = A_UTM.transform(nx_, ny_)
    arbol = cKDTree(np.column_stack([ux, uy]))
    pob = pd.read_sql("""SELECT sum(est_personas) p, (h3_cell_to_lat_lng(h3))[0] lon, (h3_cell_to_lat_lng(h3))[1] lat
                         FROM censo_2022.cruce WHERE territorio_id = 1 GROUP BY h3""", conn)
    px, py = A_UTM.transform(pob.lon.values, pob.lat.values)
    _, nodo_pob = arbol.query(np.column_stack([px, py]))
    pob_nodo = np.bincount(nodo_pob, weights=pob.p.values, minlength=n)
    hosp = pd.read_sql('SELECT ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hospitales', conn)
    hx, hy = A_UTM.transform(hosp.lon.values, hosp.lat.values)
    o_hosp = np.unique(arbol.query(np.column_stack([hx, hy]))[1])
    raiz_nodo = arbol.query(A_UTM.transform(*POSADAS))[1]

    # dependientes: árbol de caminos más cortos al hospital en seco
    g = grafo(u, v, t_seco, np.ones(len(a), bool), n)
    d_h, pred, _ = dijkstra(g, directed=False, indices=o_hosp, min_only=True, return_predecessors=True)
    sub = pob_nodo.copy()
    orden = np.argsort(-np.where(np.isfinite(d_h), d_h, -1))
    for x in orden:
        p = pred[x]
        if p >= 0 and np.isfinite(d_h[x]):
            sub[p] += sub[x]
    dep_arista = np.where(pred[v] == u, sub[v], np.where(pred[u] == v, sub[u], 0.0))
    log(f'dependientes calculados ({time.time() - t0:.0f} s)')

    cr = pd.read_sql('SELECT id, arista_id, aisla FROM infra.cruces', conn)
    pos = pd.Series(np.arange(len(a)), index=a.id.values)
    k = pos.reindex(cr.arista_id.values).values
    okk = np.isfinite(k)
    k = np.where(okk, k, 0).astype(np.int64)
    cr['dependientes_hospital'] = np.where(okk, dep_arista[k], np.nan)
    cr['intransitable_lluvia_fuerte'] = np.where(okk, peligro[k], False)

    q = np.flatnonzero(okk & (cr.dependientes_hospital.fillna(0).values > 0) & ~cr.aisla.fillna(False).values)
    log(f'desvíos a calcular: {len(q):,} cruces')
    for nombre, w in [('seco', t_seco), ('lluvia', t_lluvia)]:
        indptr, vec, peso, eid = csr_con_ids(u, v, w, n)
        t1 = time.time()
        alt = desvios(indptr, vec, peso, eid, u[k[q]].astype(np.int64), v[k[q]].astype(np.int64),
                      k[q].astype(np.int64), CORTE_MIN * 60)
        extra = (alt - w[k[q]]) / 60
        col = np.full(len(cr), np.nan)
        col[q] = np.where(np.isfinite(extra), np.maximum(extra, 0), np.nan)
        cr[f'desvio_min_{nombre}'] = col
        log(f'  desvío {nombre}: {time.time() - t1:.0f} s; mediana {np.nanmedian(col):.1f} min, '
            f'sin alternativa en {CORTE_MIN:.0f} min: {np.isnan(col[q]).sum():,}')
    cr['impacto_min_persona'] = cr.dependientes_hospital * cr.desvio_min_lluvia

    sal = pd.read_sql('''SELECT ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hospitales
                         UNION ALL SELECT ST_X(geom), ST_Y(geom) FROM infra.visor_caps''', conn)
    sx, sy = A_UTM.transform(sal.lon.values, sal.lat.values)
    salud_nodo = np.bincount(arbol.query(np.column_stack([sx, sy]))[1], minlength=n).astype(float)
    ais, ais_s = aislados(u, v, ~peligro, n, pob_nodo, raiz_nodo, salud_nodo)
    cr['personas_aisladas_lluvia_fuerte'] = np.where(okk, ais[k], np.nan)
    cr['salud_aislada_lluvia_fuerte'] = np.where(okk, ais_s[k], np.nan)
    log(f'aislamiento con lluvia fuerte: {np.isfinite(cr.personas_aisladas_lluvia_fuerte).sum():,} cruces sobre arista '
        f'de corte; ≥ 100 personas: {(cr.personas_aisladas_lluvia_fuerte >= 100).sum():,}; '
        f'≥ 1.000: {(cr.personas_aisladas_lluvia_fuerte >= 1000).sum():,} ({time.time() - t0:.0f} s)')

    cols = {'dependientes_hospital': 'real', 'desvio_min_seco': 'real', 'desvio_min_lluvia': 'real',
            'impacto_min_persona': 'real', 'personas_aisladas_lluvia_fuerte': 'real',
            'salud_aislada_lluvia_fuerte': 'real',
            'intransitable_lluvia_fuerte': 'boolean'}
    cur = conn.cursor()
    for c, tipo in cols.items():
        cur.execute(f'ALTER TABLE infra.cruces DROP COLUMN IF EXISTS {c}')
        cur.execute(f'ALTER TABLE infra.cruces ADD COLUMN {c} {tipo}')
    cur.execute('CREATE TEMP TABLE _d (id int, ' + ', '.join(f'{c} {t}' for c, t in cols.items()) + ')')
    buf = io.StringIO()
    cr[['id', *cols]].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY _d FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('UPDATE infra.cruces c SET ' + ', '.join(f'{c} = _d.{c}' for c in cols) + ' FROM _d WHERE _d.id = c.id')
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
