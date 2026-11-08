"""¿Le sirve la prensa al Hexoma? Pruebas adversariales sobre el corpus automático (7-nov-2026).

  novedad  ¿Los medios sin API (El Territorio, MisionesCuatro por muestra al azar de 300, semilla 11; La Voz de
           Cataratas entera, sólo notas que pasan el filtro v2) traen eventos que el resto del corpus no tiene?
           Mención conocida = otro medio tiene una del mismo tipo a <= 15 km y +-3 días. Regla fijada antes de mirar:
           >= 20 % de menciones nuevas. La muestra leída de nuevas está en muestra_novedad_nuevos.csv.
  A        Río Uruguay → RP2: días con algún puente de la RP2 cortado según el modelo (infra.rio_puente_diario)
           contra días con prensa de corte de puente/ruta en la RP2 (+-1 día), desde 2010.
  B        Cortes de luz contra Black Marble: menciones de corte de luz → asentamiento más cercano (<= 3 km) →
           radiancia del día y el siguiente / mediana del mismo asentamiento +-30 días; control: días al azar.

Resultados del 7-nov-2026 (patrones v2):
  novedad  nuevas 45 % El Territorio, 22 % La Voz, 26 % MisionesCuatro; reales entre las leídas 4, 5 y 5 de 10.
  A        P(prensa | modelo cortado) = 0,25; P(prensa | no cortado) = 0,017 (unas 14 veces).
  B        razón mediana 0,90 con prensa, 0,87 en control: sin señal. Por eso el corte de luz salió de la extracción.

Uso (desde neahub/):  python pipeline/noticias/probar_utilidad.py [--solo novedad,A,B]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from build_hexoma import DB, connect  # noqa: E402
from bajar_sin_api import DIR, pasa, slug_de, url_norm  # noqa: E402

NUEVOS = ('elterritorio', 'lavozdecataratas', 'misionescuatro')


def novedad(conn) -> None:
    q = lambda s, **k: pd.read_sql(s, conn, **k)  # noqa: E731
    notas = q('SELECT nota_id, medio, url FROM noticias.notas WHERE medio IN %(m)s', params={'m': NUEVOS})
    notas['clave'] = notas.url.map(url_norm)
    keep = set(notas[(notas.medio == 'lavozdecataratas') & notas.url.map(lambda u: pasa(slug_de(u)))].nota_id)
    for m in ('elterritorio', 'misionescuatro'):
        p = DIR / 'indices' / f'muestra_{m}.csv'
        claves = set(pd.read_csv(p).clave) if p.exists() else set()
        keep |= set(notas[(notas.medio == m) & notas.clave.isin(claves)].nota_id)
    cur = conn.cursor()
    cur.execute('CREATE TEMP TABLE IF NOT EXISTS _k (nota_id int)')
    cur.execute('TRUNCATE _k')
    cur.executemany('INSERT INTO _k VALUES (%s)', [(int(x),) for x in keep])
    d = q("""
        WITH nv AS (SELECT m.* FROM noticias.menciones m JOIN _k USING (nota_id) WHERE m.geom IS NOT NULL),
        viejos AS (SELECT * FROM noticias.menciones
                   WHERE medio NOT IN ('elterritorio','lavozdecataratas','misionescuatro') AND geom IS NOT NULL)
        SELECT nv.medio, nv.nota_id, nv.mencion_id, nv.tipo, nv.fecha::date fecha,
               EXISTS (SELECT 1 FROM viejos v WHERE v.tipo = nv.tipo
                       AND abs(extract(epoch FROM v.fecha - nv.fecha)) <= 3 * 86400
                       AND ST_DWithin(v.geom::geography, nv.geom::geography, 15000)) conocida
        FROM nv""")
    r = d.groupby('medio').agg(menciones=('mencion_id', 'size'), nuevas=('conocida', lambda x: int((~x).sum())))
    r['pct_nuevas'] = (r.nuevas / r.menciones * 100).round(1)
    print('novedad (menciones con lugar):')
    print(r.to_string())


def prueba_a(conn) -> None:
    q = lambda s: pd.read_sql(s, conn)  # noqa: E731
    mod = q("SELECT fecha, bool_or(cortado) cortado FROM infra.rio_puente_diario WHERE fecha >= '2010-01-01' GROUP BY 1")
    pren = q("""SELECT DISTINCT fecha::date fecha FROM noticias.menciones
                WHERE tipo IN ('corte_puente','corte_ruta_camino') AND fecha >= '2010-01-01'
                  AND (ruta ~ '(^|\\D)2(\\D|$)' OR lower(lugar_nombre) ~ 'costera')""")
    mod['fecha'] = pd.to_datetime(mod.fecha)
    p = set(pd.to_datetime(pren.fecha))
    mod['prensa'] = mod.fecha.map(lambda f: any((f + pd.Timedelta(days=k)) in p for k in (-1, 0, 1)))
    print(f'A. RP2: {len(mod):,} días; modelo cortado {int(mod.cortado.sum())}; con prensa RP2 (+-1) {int(mod.prensa.sum())}')
    print(f'   P(prensa | modelo cortado) = {mod[mod.cortado].prensa.mean():.3f}; '
          f'P(prensa | no cortado) = {mod[~mod.cortado].prensa.mean():.3f}')
    print(f'   P(modelo cortado | prensa) = {mod[mod.prensa].cortado.mean():.3f}; tasa base = {mod.cortado.mean():.3f}')


def prueba_b(conn) -> None:
    q = lambda s, **k: pd.read_sql(s, conn, **k)  # noqa: E731
    men = q("""SELECT DISTINCT ON (a.gid, m.fecha::date) a.gid, m.fecha::date fecha
               FROM noticias.menciones m
               JOIN LATERAL (SELECT gid, geom FROM infra.asentamientos ORDER BY geom <-> m.geom LIMIT 1) a ON true
               WHERE m.tipo = 'corte_luz' AND m.geom IS NOT NULL AND m.lugar_tipo IN ('asentamiento','municipio')
                 AND m.fecha >= '2012-02-01' AND ST_DWithin(a.geom::geography, m.geom::geography, 3000)""")
    if men.empty:
        print('B. sin menciones de corte de luz (salieron de la extracción el 7-nov-2026); resultado en el docstring')
        return
    luz = q("""SELECT asentamiento_gid gid, fecha, rad_media FROM infra.luz_asentamiento_diaria
               WHERE asentamiento_gid = ANY(%(g)s) AND n_validos > 0 AND rad_media IS NOT NULL""",
            params={'g': [int(x) for x in men.gid.unique()]})
    luz['fecha'] = pd.to_datetime(luz.fecha)
    luz = luz.set_index(['gid', 'fecha']).rad_media.sort_index()
    rng = np.random.default_rng(3)
    un = pd.Timedelta(days=1)

    def razon(g, f):
        try:
            s = luz.loc[g]
        except KeyError:
            return np.nan
        v = s.reindex([f, f + un]).dropna()
        ref = s[(s.index >= f - 30 * un) & (s.index <= f + 30 * un) & ((s.index < f - un) | (s.index > f + 2 * un))]
        if v.empty or len(ref) < 8 or ref.median() <= 0:
            return np.nan
        return v.min() / ref.median()

    men['fecha'] = pd.to_datetime(men.fecha)
    r = np.array([razon(g, f) for g, f in zip(men.gid, men.fecha)])
    c = np.array([razon(g, f + int(rng.integers(60, 400)) * int(rng.choice([-1, 1])) * un)
                  for g, f in zip(men.gid, men.fecha)])
    print(f'B. luz: {len(men)} pares asentamiento-día')
    for nombre, x in (('prensa', r[~np.isnan(r)]), ('control', c[~np.isnan(c)])):
        print(f'   {nombre:8s} n={len(x)} razón mediana {np.median(x):.2f}; caída > 50 %: {np.mean(x < 0.5):.0%}')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--solo', default='novedad,A,B')
    a = ap.parse_args()
    conn = connect(DB)
    pruebas = {'novedad': novedad, 'A': prueba_a, 'B': prueba_b}
    for k in a.solo.split(','):
        pruebas[k](conn)
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
