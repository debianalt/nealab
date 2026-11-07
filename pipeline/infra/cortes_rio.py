"""Crecidas reales del Uruguay sobre la red vial: qué puentes RP2 quedaron bajo agua y qué pasó con el acceso al hospital.

Eventos elegidos por datos, no a mano: los N_EVENTOS máximos anuales más altos de la escala de El Soberbio (INA 61).
Para cada evento, ventana de ±VENTANA días alrededor del pico (la onda tarda en bajar por el río); un puente queda
cortado si en esa ventana el nivel interpolado (infra.rio_puente_diario) llegó a su cota.

Red: escenario SECO de red_lluvia.py (cota pesimista) para aislar el efecto del río; se quitan las aristas de los
cruces de esos puentes (infra.cruces.puente_rp2_gid → arista_id) y se recalculan los minutos al hospital por celda.

Escribe:
  infra.rio_eventos        fecha, altura El Soberbio, puentes cortados, personas con más de 1 min extra, minutos
                           extra (mediana y P90 de los afectados, ponderados por población), personas sin ruta
  infra.rio_evento_celda   evento × celda afectada: minutos base y con el evento

Uso (desde neahub/, después de rio_uruguay.py, red_lluvia.py y aislamiento.py):
    python pipeline/infra/cortes_rio.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from red_lluvia import A_UTM, cargar_aristas, grafo, superficie, velocidades  # noqa: E402

N_EVENTOS = 8
VENTANA = 3
COTA = 'pesimista'


def ponderado(x: np.ndarray, w: np.ndarray, q: float) -> float:
    if len(x) == 0:
        return np.nan
    o = np.argsort(x)
    cw = np.cumsum(w[o])
    return float(x[o][np.searchsorted(cw, q * cw[-1])])


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    lec = pd.read_sql("SELECT fecha, altura_m FROM infra.rio_lecturas WHERE serie = 61 AND qc = 'ok'", conn,
                      parse_dates=['fecha'])
    lec['anio'] = lec.fecha.dt.year
    picos = lec.loc[lec.groupby('anio').altura_m.idxmax()].nlargest(N_EVENTOS, 'altura_m')
    diario = pd.read_sql('SELECT gid, fecha, margen_m FROM infra.rio_puente_diario', conn, parse_dates=['fecha'])
    pu = pd.read_sql('SELECT gid, nombre FROM infra.visor_puentes_rp2', conn).set_index('gid').nombre
    cru = pd.read_sql('SELECT puente_rp2_gid gid, arista_id FROM infra.cruces WHERE puente_rp2_gid IS NOT NULL', conn)

    a = cargar_aristas(conn)
    clase = a.clase.values.astype(str)
    sup = superficie(clase, a.superficie.values.astype(str), COTA)
    vs, _ = velocidades(clase, sup)
    t_seco = a.largo_m.values / (vs / 3.6)
    ids, inv = np.unique(np.concatenate([a.source.values, a.target.values]), return_inverse=True)
    u, v = inv[:len(a)], inv[len(a):]
    n = len(ids)
    nx_, ny_ = np.empty(n), np.empty(n)
    nx_[u], ny_[u] = a.x1.values, a.y1.values
    nx_[v], ny_[v] = a.x2.values, a.y2.values
    arbol = cKDTree(np.column_stack(A_UTM.transform(nx_, ny_)))
    pob = pd.read_sql("""SELECT h3::text h3, sum(est_personas) p, (h3_cell_to_lat_lng(h3))[0] lon,
                                (h3_cell_to_lat_lng(h3))[1] lat
                         FROM censo_2022.cruce WHERE territorio_id = 1 GROUP BY h3 HAVING sum(est_personas) > 0""", conn)
    _, nodo_pob = arbol.query(np.column_stack(A_UTM.transform(pob.lon.values, pob.lat.values)))
    hosp = pd.read_sql('SELECT ST_X(geom) lon, ST_Y(geom) lat FROM infra.visor_hospitales', conn)
    o_hosp = np.unique(arbol.query(np.column_stack(A_UTM.transform(hosp.lon.values, hosp.lat.values)))[1])
    pos = pd.Series(np.arange(len(a)), index=a.id.values)

    todas = np.ones(len(a), bool)
    base = dijkstra(grafo(u, v, t_seco, todas, n), directed=False, indices=o_hosp, min_only=True)[nodo_pob] / 60
    eventos, celdas = [], []
    for e in picos.sort_values('fecha').itertuples():
        w = diario[(diario.fecha >= e.fecha - pd.Timedelta(days=VENTANA)) & (diario.fecha <= e.fecha + pd.Timedelta(days=VENTANA))]
        mx = w.groupby('gid').margen_m.max()
        cortados = mx[mx >= 0].index.tolist()
        keep = todas.copy()
        idx = pos.reindex(cru[cru.gid.isin(cortados)].arista_id.values).dropna().astype(int).values
        keep[idx] = False
        d = dijkstra(grafo(u, v, t_seco, keep, n), directed=False, indices=o_hosp, min_only=True)[nodo_pob] / 60
        sin = ~np.isfinite(d) & np.isfinite(base)
        extra = np.where(np.isfinite(d), d - base, np.nan)
        af = np.isfinite(extra) & (extra > 1)
        eventos.append(dict(fecha=e.fecha.date(), altura_el_soberbio_m=e.altura_m, n_puentes_cortados=len(cortados),
                            puentes_cortados='; '.join(pu.get(g, str(g)) for g in cortados),
                            puentes_evaluados=int(mx.notna().sum()),
                            personas_con_mas_minutos=float(pob.p.values[af].sum()),
                            min_extra_mediana=ponderado(extra[af], pob.p.values[af], .5),
                            min_extra_p90=ponderado(extra[af], pob.p.values[af], .9),
                            personas_sin_ruta=float(pob.p.values[sin].sum())))
        m = af | sin
        celdas.append(pd.DataFrame({'fecha': e.fecha.date(), 'h3': pob.h3.values[m], 'personas': pob.p.values[m],
                                    'min_base': base[m], 'min_evento': np.where(np.isfinite(d[m]), d[m], np.nan)}))
        log(f'  {e.fecha:%Y-%m-%d} El Soberbio {e.altura_m:5.2f} m: {len(cortados)} puentes cortados; '
            f'{eventos[-1]["personas_con_mas_minutos"]:,.0f} personas con más minutos (mediana '
            f'{eventos[-1]["min_extra_mediana"]:.1f}); sin ruta {eventos[-1]["personas_sin_ruta"]:,.0f}')

    ev = pd.DataFrame(eventos)
    ce = pd.concat(celdas, ignore_index=True)
    cur = conn.cursor()
    for t, df, ddl, com in [
        ('infra.rio_eventos', ev, 'fecha date, altura_el_soberbio_m real, n_puentes_cortados int, puentes_cortados text, '
         'puentes_evaluados int, personas_con_mas_minutos real, min_extra_mediana real, min_extra_p90 real, '
         'personas_sin_ruta real',
         f'Los {N_EVENTOS} máximos anuales más altos de El Soberbio (INA 61) sobre la red: puentes RP2 bajo su cota en '
         f'±{VENTANA} días y efecto en minutos al hospital (escenario seco; cortes_rio.py)'),
        ('infra.rio_evento_celda', ce, 'fecha date, h3 h3index, personas real, min_base real, min_evento real',
         'Celdas con más de 1 min extra o sin ruta al hospital en cada evento de infra.rio_eventos')]:
        cur.execute(f'DROP TABLE IF EXISTS {t}')
        cur.execute(f'CREATE TABLE {t} ({ddl})')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
        cur.execute(f'COMMENT ON TABLE {t} IS %s', (com,))
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
