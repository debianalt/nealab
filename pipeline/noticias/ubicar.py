"""Ubicación de cada mención de prensa: el punto más exacto que permite el texto, con su fuente (noticias v3).

Lee los nombres que dejó extraer.py y prueba, en el orden de ORDEN según el tipo de evento:

  cruce            arroyo X y ruta R en la misma oración → intersección de infra.arroyos (nombre X) con vial.caminos
                   (ref R), la más cercana al lugar de referencia
  puente           puente con nombre propio (noticias.toponimos, clase puente)
  cruce_inferido   (corte de puente) arroyo X sin ruta → el cruce de infra.cruces sobre X más cercano al lugar de
                   referencia (≤ 25 km)
  km               ruta R y km K → el mojón de km K (±1) de infra.visor_progresivas pegado (≤ 60 m) a la ruta R
  hidrometro       (crecida) el hidrómetro nombrado
  cauce_localidad  (crecida, evacuados) arroyo X y un lugar → el punto de X más cercano al lugar (≤ 15 km)
  paraje, barrio, picada, escuela   noticias.toponimos (toponimos.py); entre homónimos, el más cercano a la referencia
                   (≤ 30 km)
  nominatim        nombre nuevo («paraje X», «barrio X» …) buscado en Nominatim (OSM) dentro de Misiones; caché en
                   noticias.geocodificacion; 1 pedido por segundo, User-Agent que dice quiénes somos
  manual           pipeline/noticias/toponimos_manual.csv (nombre, lon, lat, url): lugares buscados a mano, con la
                   página que los sostiene
  ruta_localidad   (corte de ruta o puente) ruta R y un lugar → el punto de R más cercano al lugar (≤ 15 km)
  asentamiento, municipio   el lugar nombrado (BAHRA / visor), asentamiento antes que municipio
  cauce            el arroyo nombrado solo (punto medio del cauce: precisión baja)
  contexto_nota    sin nombre en la oración → el lugar más nombrado de la nota (precisión más baja)

Lugar de referencia = el primer asentamiento o municipio de la oración; si no hay, el contexto de la nota.
Sólo Misiones: un punto fuera de la provincia (unión de infra.visor_municipios + 500 m) se descarta y la mención
se borra.

Agrega a noticias.menciones: precision_lugar, lugar_nombre, lugar_ref, fuente_ubicacion, ambiguo, geom.

Uso (desde neahub/, después de extraer.py):  python pipeline/noticias/ubicar.py [--sin-nominatim]
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from shapely import wkt as swkt
from shapely.geometry import Point
from shapely.prepared import prep

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from build_hexoma import DB, connect, log  # noqa: E402
from extraer import clave, nomenclador  # noqa: E402

UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar; investigacion CONICET)'}
NOMINATIM = 'https://nominatim.openstreetmap.org/search'
VIEWBOX = '-56.11,-25.44,-53.59,-28.21'
MANUAL = ROOT / 'pipeline' / 'noticias' / 'toponimos_manual.csv'
# v3.1: ninguna ruta «cruza» el Uruguay, el Paraná ni el Iguazú salvo los puentes internacionales (que van por nombre):
# «crecida del río Uruguay y del arroyo Pindaytí, corte en la RP 2» es el Pindaytí
RIOS_FRONTERA = {'uruguai', 'parana', 'iguazu', 'paraguai', 'pepiri guazu', 'pepiri-guazu'}
ORDEN = {
    'corte_puente': ['cruce', 'puente', 'cruce_inferido', 'km', 'manual', 'toponimo', 'nominatim', 'ruta_localidad',
                     'lugar', 'cauce', 'contexto_nota'],
    'corte_ruta_camino': ['km', 'cruce', 'puente', 'manual', 'toponimo', 'nominatim', 'ruta_localidad', 'lugar',
                          'cauce', 'contexto_nota'],
    'crecida': ['hidrometro', 'cauce_localidad', 'manual', 'toponimo', 'nominatim', 'lugar', 'cauce', 'contexto_nota'],
    'evacuados_aislados': ['manual', 'toponimo', 'nominatim', 'lugar', 'cauce_localidad', 'cauce', 'contexto_nota'],
}


def ref_vial(r: str) -> str:
    return r.replace(' ', '')                     # «RP 2» → «RP2» (vial.caminos.ref)


def km_de(p) -> float | None:
    try:
        return float(str(p).replace(',', '.'))
    except ValueError:
        return None


class Ubicador:
    def __init__(self, conn, nominatim: bool):
        self.conn, self.nominatim = conn, nominatim
        q = lambda s, **k: pd.read_sql(s, conn, **k)  # noqa: E731
        g = nomenclador(conn)
        self.base = {k: grp for k, grp in g[g.tipo != 'cauce'].groupby('clave')}
        self.cauce_pt = {r.clave: (r.lon, r.lat, r.nombre) for r in g[g.tipo == 'cauce'].itertuples()}
        t = q("SELECT toponimo_id, clase, nombre, clave, fuente, ref, ST_X(geom) lon, ST_Y(geom) lat FROM noticias.toponimos")
        t['clave'] = t.clave.str.replace(r'^(barrio|b°|bº|paraje)\s+', '', regex=True)
        self.topo = {k: grp for k, grp in t.groupby('clave')}
        nombres = q("SELECT DISTINCT nombre FROM infra.arroyos WHERE nombre ~* '^(arroyo|r[ií]o)\\s'")
        nombres['clave'] = nombres.nombre.str.replace(r'(?i)^(Arroyo|Río|Rio|A°|Aº)\s+', '', regex=True).map(clave)
        self.arroyo_nombres = nombres.groupby('clave').nombre.apply(list).to_dict()
        mis = q("SELECT ST_AsText(ST_Buffer(ST_Union(geom)::geography, 500)::geometry) w FROM infra.visor_municipios").w[0]
        self.misiones = prep(swkt.loads(mis))
        hit = q("""SELECT p.gid, p.props->>'distance' dist, p.props->>'ref' pref, c.ref, ST_X(p.geom) lon, ST_Y(p.geom) lat
                   FROM infra.visor_progresivas p
                   JOIN LATERAL (SELECT ref FROM vial.caminos c WHERE c.ref IS NOT NULL
                                 AND ST_DWithin(c.geom::geography, p.geom::geography, 60)
                                 ORDER BY c.geom <-> p.geom LIMIT 1) c ON true""")
        hit['km'] = hit.dist.map(km_de)
        hit.loc[hit.km.isna(), 'km'] = hit.loc[hit.km.isna(), 'pref'].str.extract(r'(\d+(?:[.,]\d+)?)', expand=False).map(km_de)
        self.hitos = hit.dropna(subset=['km'])
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS noticias.geocodificacion (consulta text PRIMARY KEY, lon double precision,
                       lat double precision, osm_ref text, nombre text, respuesta jsonb, fecha timestamp DEFAULT now())""")
        conn.commit()
        cur.execute('SELECT consulta, respuesta FROM noticias.geocodificacion')
        self.geo = {c: (r if isinstance(r, list) else json.loads(r or '[]')) for c, r in cur.fetchall()}   # respuesta cruda
        self.manual = {}      # cada alias (clave) → la fila; buscados a mano, con la URL que los sostiene
        if MANUAL.exists():
            for r in pd.read_csv(MANUAL).dropna(subset=['lon', 'lat']).itertuples():
                for a in [r.nombre] + str(r.alias).split('|'):
                    self.manual[clave(a)] = r
        self._cache: dict = {}
        self._ultimo = 0.0

    # ---------- piezas
    def lugar_base(self, k: str, ref: Point | None, tipos=('asentamiento', 'municipio')):
        g = self.base.get(k)
        if g is None:
            return None
        g = g[g.tipo.isin(tipos)]
        if g.empty:
            return None
        if ref is not None and len(g) > 1:
            g = g.assign(d=((g.lon - ref.x) ** 2 + (g.lat - ref.y) ** 2) ** 0.5).sort_values('d')
        else:
            g = g.sort_values('tipo')                       # asentamiento antes que municipio
        r = g.iloc[0]
        tabla = {'asentamiento': 'infra.asentamientos', 'municipio': 'infra.visor_municipios', 'hidrometro': 'infra.visor_hidrometros'}
        return dict(precision=r.tipo, lugar_nombre=r.nombre, lugar_ref=r.ref, fuente_ubicacion=f'{tabla[r.tipo]} gid {r.ref}',
                    ambiguo=bool(len(g) > 1 and ref is None), lon=r.lon, lat=r.lat)

    def referencia(self, lugares: list[str], contexto: str | None) -> Point | None:
        for k in lugares + ([contexto] if contexto else []):
            r = self.lugar_base(k, None)
            if r:
                return Point(r['lon'], r['lat'])
        return None

    def sql1(self, clave_cache, sql, params):
        if clave_cache not in self._cache:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self._cache[clave_cache] = cur.fetchone()
        return self._cache[clave_cache]

    def cruce(self, cauce, ruta, ref):
        nombres = self.arroyo_nombres.get(cauce)
        if not nombres or not ruta or cauce in RIOS_FRONTERA:
            return None
        x, y = (ref.x, ref.y) if ref is not None else (None, None)
        r = self.sql1(('cruce', cauce, ruta, x, y), """
            SELECT ST_X(p), ST_Y(p), a.arroyo_id, c.id, count(*) OVER () FROM infra.arroyos a
            JOIN vial.caminos c ON c.ref = %(r)s AND ST_Intersects(a.geom, c.geom)
            CROSS JOIN LATERAL (SELECT (ST_Dump(ST_Intersection(a.geom, c.geom))).geom p) w
            WHERE a.nombre = ANY(%(n)s) AND GeometryType(p) = 'POINT'
            ORDER BY CASE WHEN %(x)s::float IS NULL THEN 0 ELSE ST_Distance(p, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)) END
            LIMIT 1""", {'r': ref_vial(ruta), 'n': nombres, 'x': x, 'y': y})
        if not r:
            return None
        return dict(precision='cruce', lugar_nombre=f'{self.cauce_pt.get(cauce, (0, 0, cauce))[2]} × {ruta}',
                    lugar_ref=f'{r[2]}x{r[3]}', fuente_ubicacion=f'infra.arroyos {r[2]} × vial.caminos {r[3]} ({ruta})',
                    ambiguo=bool(r[4] > 1 and ref is None), lon=r[0], lat=r[1])

    def cruce_inferido(self, cauce, ref):
        nombres = self.arroyo_nombres.get(cauce)
        if not nombres or ref is None or cauce in RIOS_FRONTERA:
            return None
        r = self.sql1(('cinf', cauce, ref.x, ref.y), """
            SELECT ST_X(geom), ST_Y(geom), id, coalesce(nombre_camino, ref_camino) FROM infra.cruces
            WHERE cauce_nombre = ANY(%(n)s)
              AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)::geography, 25000)
            ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326) LIMIT 1""", {'n': nombres, 'x': ref.x, 'y': ref.y})
        if not r:
            return None
        return dict(precision='cruce_inferido', lugar_nombre=f'{self.cauce_pt.get(cauce, (0, 0, cauce))[2]} × {r[3] or "camino"}',
                    lugar_ref=str(r[2]), fuente_ubicacion=f'infra.cruces id {r[2]} (el más cercano a la referencia)',
                    ambiguo=False, lon=r[0], lat=r[1])

    def cauce_localidad(self, cauce, ref):
        nombres = self.arroyo_nombres.get(cauce)
        if not nombres or ref is None:
            return None
        r = self.sql1(('cloc', cauce, ref.x, ref.y), """
            SELECT ST_X(p), ST_Y(p), arroyo_id FROM (
              SELECT ST_ClosestPoint(geom, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)) p, arroyo_id FROM infra.arroyos
              WHERE nombre = ANY(%(n)s) ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326) LIMIT 1) z
            WHERE ST_DWithin(p::geography, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)::geography, 15000)""",
                     {'n': nombres, 'x': ref.x, 'y': ref.y})
        if not r:
            return None
        return dict(precision='cauce_localidad', lugar_nombre=self.cauce_pt.get(cauce, (0, 0, cauce))[2], lugar_ref=str(r[2]),
                    fuente_ubicacion=f'infra.arroyos {r[2]} (punto más cercano a la referencia)', ambiguo=False, lon=r[0], lat=r[1])

    def ruta_localidad(self, ruta, ref):
        if not ruta or ref is None:
            return None
        r = self.sql1(('rloc', ruta, ref.x, ref.y), """
            SELECT ST_X(p), ST_Y(p), id FROM (
              SELECT ST_ClosestPoint(geom, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)) p, id FROM vial.caminos
              WHERE ref = %(r)s ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326) LIMIT 1) z
            WHERE ST_DWithin(p::geography, ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4326)::geography, 15000)""",
                     {'r': ref_vial(ruta), 'x': ref.x, 'y': ref.y})
        if not r:
            return None
        return dict(precision='ruta_localidad', lugar_nombre=ruta, lugar_ref=str(r[2]),
                    fuente_ubicacion=f'vial.caminos {r[2]} ({ruta}, punto más cercano a la referencia)', ambiguo=False, lon=r[0], lat=r[1])

    def km(self, ruta, km):
        k = km_de(km) if km else None
        if not ruta or k is None:
            return None
        h = self.hitos[(self.hitos.ref == ref_vial(ruta)) & ((self.hitos.km - k).abs() <= 1)]
        if h.empty:
            return None
        r = h.assign(d=(h.km - k).abs()).sort_values('d').iloc[0]
        return dict(precision='km', lugar_nombre=f'{ruta} km {k:g}', lugar_ref=str(r.gid),
                    fuente_ubicacion=f'infra.visor_progresivas gid {r.gid} (mojón km {r.km:g})', ambiguo=False, lon=r.lon, lat=r.lat)

    def toponimo(self, k, ref, clases=('paraje', 'barrio', 'picada', 'escuela', 'puente')):
        g = self.topo.get(k)
        if g is None:
            return None
        g = g[g.clase.isin(clases)]
        if g.empty:
            return None
        if ref is not None:
            g = g.assign(d=((g.lon - ref.x) ** 2 + (g.lat - ref.y) ** 2) ** 0.5).sort_values('d')
            if g.d.iloc[0] > 0.3:                          # ~30 km: homónimo de otro lado
                return None
        elif len(g) > 1 and g.clase.iloc[0] in ('barrio', 'escuela'):
            return None                                     # «barrio San Jorge» sin referencia: hay muchos
        r = g.iloc[0]
        return dict(precision=r.clase, lugar_nombre=r.nombre, lugar_ref=str(r.toponimo_id),
                    fuente_ubicacion=f'noticias.toponimos {r.toponimo_id} ({r.fuente} {r.ref})',
                    ambiguo=bool(len(g) > 1 and ref is None), lon=r.lon, lat=r.lat)

    def elegir(self, cand: str, res: list):
        """El primer resultado dentro de Misiones cuyo nombre contiene TODAS las palabras propias del buscado
        (Nominatim aproxima: «Kilómetro 517» → «Kilómetro 130», «2000 Hectáreas» → «Virgen de Guadalupe»)."""
        nucleo = [w for w in clave(cand.split(' ', 1)[-1]).split() if w not in ('de', 'del', 'la', 'las', 'los', 'el')]
        for x in res:
            nombre = clave(', '.join(x.get('display_name', '').split(', ')[:2]))
            if all(w in nombre.split() for w in nucleo) and self.misiones.contains(Point(float(x['lon']), float(x['lat']))):
                return x
        return None

    def geocodificar(self, cand: str):
        if cand in self.geo:
            hit = self.elegir(cand, self.geo[cand])
            return (float(hit['lon']), float(hit['lat']), f"{hit['osm_type']}/{hit['osm_id']}", hit['display_name']) if hit else None
        if not self.nominatim:
            return None
        espera = self._ultimo + 1.1 - time.time()
        if espera > 0:
            time.sleep(espera)
        self._ultimo = time.time()
        try:
            rr = requests.get(NOMINATIM, headers=UA, timeout=60, params={
                'q': f'{cand}, Misiones, Argentina', 'format': 'jsonv2', 'limit': 3, 'countrycodes': 'ar',
                'viewbox': VIEWBOX, 'bounded': 1})
            res = rr.json() if rr.status_code == 200 else []
        except (requests.RequestException, ValueError):
            return None
        hit = self.elegir(cand, res)
        fila = (cand, float(hit['lon']) if hit else None, float(hit['lat']) if hit else None,
                f"{hit['osm_type']}/{hit['osm_id']}" if hit else None, hit.get('display_name') if hit else None,
                json.dumps(res, ensure_ascii=False))
        cur = self.conn.cursor()
        cur.execute('INSERT INTO noticias.geocodificacion (consulta, lon, lat, osm_ref, nombre, respuesta) '
                    'VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (consulta) DO NOTHING', fila)
        self.conn.commit()
        self.geo[cand] = res
        return (fila[1], fila[2], fila[3], fila[4]) if hit else None

    # ---------- una mención
    def ubicar(self, m) -> dict | None:
        sp = lambda s: [x for x in (s or '').split('|') if x]  # noqa: E731
        lugares, cauces, topo, cand = sp(m.lugares), sp(m.cauces), sp(m.toponimos), sp(m.candidatos)
        no_hid = [x for x in lugares if x != m.hidrometro]
        ref = self.referencia(no_hid, m.contexto)
        for paso in ORDEN[m.tipo]:
            r = None
            if paso == 'cruce':
                r = next((x for c in cauces if (x := self.cruce(c, m.ruta, ref))), None)
            elif paso == 'puente':
                r = next((x for k in topo if (x := self.toponimo(k, ref, ('puente',)))), None)
            elif paso == 'cruce_inferido':
                r = next((x for c in cauces if (x := self.cruce_inferido(c, ref))), None)
            elif paso == 'km':
                r = self.km(m.ruta, m.km)
            elif paso == 'hidrometro' and m.hidrometro:
                r = self.lugar_base(m.hidrometro, ref, ('hidrometro',))
            elif paso == 'cauce_localidad':
                r = next((x for c in cauces if (x := self.cauce_localidad(c, ref))), None)
            elif paso == 'toponimo':
                r = next((x for k in topo if (x := self.toponimo(k, ref, ('paraje', 'barrio', 'picada', 'escuela')))), None)
            elif paso == 'nominatim':
                for c in cand:
                    g = self.geocodificar(c)
                    if g:
                        r = dict(precision='nominatim', lugar_nombre=c, lugar_ref=g[2],
                                 fuente_ubicacion=f'Nominatim (OSM {g[2]}: {g[3]})', ambiguo=False, lon=g[0], lat=g[1])
                        break
            elif paso == 'manual':
                for c in cand + topo:
                    x = self.manual.get(clave(c)) or self.manual.get(clave(c.split(' ', 1)[-1]))
                    lejos = (x is not None and ref is not None
                             and ((x.lon - ref.x) ** 2 + (x.lat - ref.y) ** 2) ** 0.5 > 0.4)   # ~40 km: homónimo
                    if x is not None and not lejos:
                        r = dict(precision='manual', lugar_nombre=x.nombre, lugar_ref=x.url,
                                 fuente_ubicacion=f'manual: {x.url}', ambiguo=False, lon=x.lon, lat=x.lat)
                        break
            elif paso == 'ruta_localidad':
                r = self.ruta_localidad(m.ruta, ref) if no_hid else None
            elif paso == 'lugar':
                r = next((x for k in no_hid if (x := self.lugar_base(k, ref))), None)
            elif paso == 'cauce' and cauces:
                c = cauces[0]
                if c in self.cauce_pt:
                    lon, lat, nombre = self.cauce_pt[c]
                    r = dict(precision='cauce', lugar_nombre=nombre, lugar_ref=c,
                             fuente_ubicacion='infra.arroyos (punto medio del cauce)', ambiguo=False, lon=lon, lat=lat)
            elif paso == 'contexto_nota' and m.contexto:
                r = self.lugar_base(m.contexto, None, ('asentamiento', 'municipio'))
                if r:
                    r['precision'] = 'contexto_nota'
                    r['fuente_ubicacion'] += ' (lugar más nombrado de la nota)'
            if r and r.get('lon') is not None and not pd.isna(r['lon']):
                r['fuera'] = not self.misiones.contains(Point(r['lon'], r['lat']))
                return r
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--sin-nominatim', action='store_true')
    a = ap.parse_args()
    t0 = time.time()
    conn = connect(DB)
    men = pd.read_sql("""SELECT mencion_id, tipo, ruta, km, hidrometro, cauces, lugares, toponimos, candidatos, contexto
                         FROM noticias.menciones""", conn)
    u = Ubicador(conn, not a.sin_nominatim)
    log(f'ubicando {len(men):,} menciones; {len(u.hitos):,} mojones con ruta; {len(u.geo):,} consultas en caché')
    filas = []
    for i, m in enumerate(men.itertuples()):
        r = u.ubicar(m)
        if r:
            filas.append(dict(r, mencion_id=m.mencion_id))
        if i % 5000 == 0:
            log(f'  {i:,}/{len(men):,} ({time.time() - t0:.0f} s)')
    d = pd.DataFrame(filas)
    cur = conn.cursor()
    for c, tipo in [('precision_lugar', 'text'), ('lugar_nombre', 'text'), ('lugar_ref', 'text'), ('fuente_ubicacion', 'text'),
                    ('ambiguo', 'boolean')]:
        cur.execute(f'ALTER TABLE noticias.menciones ADD COLUMN IF NOT EXISTS {c} {tipo}')
    cur.execute('ALTER TABLE noticias.menciones ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)')
    cur.execute('CREATE TEMP TABLE _u (mencion_id int, precision_lugar text, lugar_nombre text, lugar_ref text, '
                'fuente_ubicacion text, ambiguo boolean, lon double precision, lat double precision, fuera boolean)')
    buf = io.StringIO()
    d = d.rename(columns={'precision': 'precision_lugar'})
    d[['mencion_id', 'precision_lugar', 'lugar_nombre', 'lugar_ref', 'fuente_ubicacion', 'ambiguo', 'lon', 'lat', 'fuera']].to_csv(
        buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY _u FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""UPDATE noticias.menciones m SET precision_lugar = u.precision_lugar, lugar_nombre = u.lugar_nombre,
                   lugar_ref = u.lugar_ref, fuente_ubicacion = u.fuente_ubicacion, ambiguo = u.ambiguo,
                   geom = ST_SetSRID(ST_MakePoint(u.lon, u.lat), 4326)
                   FROM _u u WHERE u.mencion_id = m.mencion_id""")
    cur.execute('DELETE FROM noticias.menciones m USING _u u WHERE u.mencion_id = m.mencion_id AND u.fuera')
    fuera = cur.rowcount
    cur.execute('CREATE INDEX IF NOT EXISTS menciones_geom_idx ON noticias.menciones USING gist (geom)')
    conn.commit()
    p = pd.read_sql("SELECT coalesce(precision_lugar, 'sin lugar') AS precision_lugar, count(*) n FROM noticias.menciones "
                    "GROUP BY 1 ORDER BY 2 DESC", conn)
    conn.close()
    log(f'fuera de Misiones (borradas): {fuera:,}; listo en {time.time() - t0:.0f} s\n' + p.to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
