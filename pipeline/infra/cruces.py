"""Universo de cruces camino × cauce de Misiones → `infra.cruces`.

Un cruce es un lugar donde un camino de `vial.caminos` pasa sobre un cauce. Se detecta de tres maneras y se
unen:

1. Terreno (drenaje.py): cada camino se muestrea cada PASO_M metros; en cada punto se lee el máximo del área
   drenada en ±1 celda (±30 m, para absorber el desfasaje entre el trazado del camino y el cauce del modelo de
   elevación). Un cruce es un pico de esa serie a lo largo del camino con al menos A_MIN km² drenados
   (scipy.signal.find_peaks sobre log(área): prominencia ×2, picos separados ≥ 150 m). Un camino que corre por
   el fondo de un valle da una racha larga sobre el umbral: se marca `paralelo` (racha > 300 m). Los picos
   crudos quedan en `infra.cruces_terreno` (validación).
2. OSM: intersección de cada camino con `infra.osm_cauces` (cauces mapeados; incluye los tramos
   alcantarillados, que en OSM son el cauce bajo el camino).
3. Estructuras: puentes vehiculares de OSM, alcantarillas y vados de OSM, puentes y alcantarillas del visor,
   llevados al camino más cercano a ≤ 50 m.

Unión: todo punto con todo punto a ≤ UNION_M; además cada pico de terreno se ancla al punto OSM o de estructura
más cercano de su camino (o de uno que lo toca: en OSM el puente es su propio tramo) a ≤ SNAP_M, y viceversa
para los puntos OSM o de estructura sin terreno cerca. El cauce de FABDEM 30 m queda corrido ~100 m (mediana)
del puente que OSM dibujó sobre imagen (validacion_cruces.md), así que la ubicación del cruce es la de la
estructura, si no la de la intersección OSM, si no el pico; `desplazamiento_m` dice cuánto se movió. El área
drenada es la mayor del grupo.

Evidencia de qué estructura hay (a ≤ 50 m; RP2 a ≤ 100 m):
  puente   vial.osm con bridge, infra.visor_puentes, infra.visor_puentes_rp2 (con su cota de seguridad)
  vado     infra.osm_estructuras tipo 'vado'
  alcantarilla  infra.osm_estructuras tipo 'alcantarilla', infra.visor_alcantarillas
Clase `estructura`: puente > vado > alcantarilla > sin_dato. `n_fuentes_puente` cuenta cuántas fuentes
independientes lo confirman (OSM y el visor de puentes cuentan como una: el visor sale de OSM).

Uso (desde neahub/, después de drenaje.py, importar_visor.py y cauces_osm.py):
    python pipeline/infra/cruces.py
"""
from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from pyproj import Transformer
from scipy.ndimage import maximum_filter
from scipy.signal import find_peaks
from scipy.spatial import cKDTree

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402  (limpia PROJ_LIB antes de rasterio)
from build_hexoma import DB, connect, log  # noqa: E402

ACC = drenaje.FUENTES / 'acc_km2_misiones.tif'
PASO_M = 15.0
A_MIN = 0.1          # km², mismo umbral de red que la demo El Soberbio
PARALELO_M = 300.0
UNION_M = 60.0
SNAP_M = 150.0      # corrimiento máximo cauce FABDEM ↔ estructura OSM sobre el mismo camino
A_UTM = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)
A_GEO = Transformer.from_crs('EPSG:32721', 'EPSG:4326', always_xy=True)


def leer_acc():
    import rasterio
    with rasterio.open(ACC) as src:
        a = src.read(1)
        tr = src.transform
    a = np.where(np.isfinite(a), a, 0).astype(np.float32)
    return maximum_filter(a, size=3), tr


def valores(acc, tr, lon, lat):
    col = np.floor((lon - tr.c) / tr.a).astype(np.int64)
    fil = np.floor((lat - tr.f) / tr.e).astype(np.int64)
    ok = (fil >= 0) & (fil < acc.shape[0]) & (col >= 0) & (col < acc.shape[1])
    v = np.zeros(lon.shape, dtype=np.float32)
    v[ok] = acc[fil[ok], col[ok]]
    return v


def caminos(conn) -> pd.DataFrame:
    return pd.read_sql("""SELECT id, clase, superficie, nombre, ref, ST_AsBinary(geom) AS wkb
                          FROM vial.caminos WHERE provincia = 'misiones'""", conn)


def por_terreno(cam: pd.DataFrame, acc, tr) -> pd.DataFrame:
    geo = shapely.from_wkb(cam.wkb.map(bytes).values)
    geo = shapely.line_merge(geo)  # MultiLineString de un solo tramo → LineString
    utm = shapely.transform(geo, lambda xy: np.column_stack(A_UTM.transform(xy[:, 0], xy[:, 1])))
    largo = shapely.length(utm)
    n = np.maximum(np.ceil(largo / PASO_M).astype(int), 1) + 1
    idx = np.repeat(np.arange(len(utm)), n)
    off = np.arange(n.sum()) - np.repeat(np.cumsum(n) - n, n)
    dist = np.minimum(off * PASO_M, np.repeat(largo, n))
    pts = shapely.line_interpolate_point(utm[idx], dist)
    x, y = shapely.get_x(pts), shapely.get_y(pts)
    lon, lat = A_GEO.transform(x, y)
    v = valores(acc, tr, np.asarray(lon), np.asarray(lat))
    log(f'  {len(v):,} puntos de muestreo en {len(utm):,} caminos ({largo.sum() / 1000:,.0f} km)')
    filas = []
    ini = np.cumsum(n) - n
    lv = np.log(np.maximum(v, 1e-4))
    for i in range(len(utm)):
        s = slice(ini[i], ini[i] + n[i])
        vv = v[s]
        if vv.max() < A_MIN:
            continue
        serie = np.concatenate([[np.log(1e-4)], lv[s], [np.log(1e-4)]])  # bordes bajos: un pico en la punta cuenta
        picos, _ = find_peaks(serie, height=np.log(A_MIN), prominence=np.log(2), distance=int(150 / PASO_M))
        sobre = vv >= A_MIN
        for p in picos - 1:
            # racha sobre el umbral que contiene al pico
            a = p
            while a > 0 and sobre[a - 1]:
                a -= 1
            b = p
            while b < len(vv) - 1 and sobre[b + 1]:
                b += 1
            j = ini[i] + p
            filas.append((int(cam.id.iat[i]), float(lon[j]), float(lat[j]), float(vv[p]), (b - a + 1) * PASO_M))
    return pd.DataFrame(filas, columns=['camino_id', 'lon', 'lat', 'area_drenada_km2', 'racha_m'])


def por_osm(conn, acc, tr) -> pd.DataFrame:
    d = pd.read_sql("""
        SELECT c.id AS camino_id, ST_X(p.geom) AS lon, ST_Y(p.geom) AS lat
        FROM vial.caminos c
        JOIN infra.osm_cauces w ON ST_Intersects(c.geom, w.geom)
        CROSS JOIN LATERAL ST_Dump(ST_Intersection(c.geom, w.geom)) p
        WHERE c.provincia = 'misiones' AND ST_GeometryType(p.geom) = 'ST_Point'""", conn)
    d['area_drenada_km2'] = valores(acc, tr, d.lon.values, d.lat.values)
    d['racha_m'] = np.nan
    return d


VEHICULAR = ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary',
             'secondary_link', 'tertiary', 'tertiary_link', 'unclassified', 'road', 'track', 'residential',
             'living_street', 'service')


def por_estructura(conn, acc, tr) -> pd.DataFrame:
    """Puentes vehiculares de OSM, alcantarillas y vados de OSM, puentes y alcantarillas del visor, llevados al
    punto más cercano de un camino de Misiones a ≤ 50 m. Son la ubicación más precisa de un cruce: OSM los
    dibuja sobre imagen, y el cauce de FABDEM 30 m queda corrido ~100 m (mediana; validacion_cruces.md)."""
    vh = ', '.join(f"'{h}'" for h in VEHICULAR)
    d = pd.read_sql(f"""
        WITH s AS (
          SELECT 1 prio, 'osm_puente' src, osm_id::bigint sid, ST_PointOnSurface(geom) g FROM vial.osm
            WHERE provincia = 'misiones' AND tags ? 'bridge' AND tags->>'bridge' <> 'no' AND highway IN ({vh})
          UNION ALL SELECT 1, 'visor_puente', gid, geom FROM infra.visor_puentes
          UNION ALL SELECT 2, 'osm_' || tipo, gid, geom FROM infra.osm_estructuras
          UNION ALL SELECT 2, 'visor_alcantarilla', gid, geom FROM infra.visor_alcantarillas)
        SELECT DISTINCT ON (s.src, s.sid) s.prio, s.src, k.id AS camino_id,
               ST_X(ST_ClosestPoint(k.geom, s.g)) lon, ST_Y(ST_ClosestPoint(k.geom, s.g)) lat
        FROM s JOIN vial.caminos k ON k.provincia = 'misiones' AND ST_DWithin(k.geom, s.g, 0.0006)
             AND ST_DWithin(k.geom::geography, s.g::geography, 50)
        ORDER BY s.src, s.sid, k.geom <-> s.g""", conn)
    d['area_drenada_km2'] = valores(acc, tr, d.lon.values, d.lat.values)
    d['racha_m'] = np.nan
    return d


def adyacencia(conn) -> np.ndarray:
    """Claves a·10⁷+b (a < b) de los pares de caminos de Misiones que se tocan o se cruzan. En OSM un puente
    suele ser su propio tramo de vía: el pico de terreno puede caer en el tramo de acceso, otro camino_id."""
    d = pd.read_sql("""SELECT a.id a, b.id b FROM vial.caminos a JOIN vial.caminos b
                       ON a.provincia = 'misiones' AND b.provincia = 'misiones' AND a.id < b.id
                       AND ST_DWithin(a.geom, b.geom, 0.000005)""", conn)
    return np.unique(d.a.values.astype(np.int64) * 10_000_000 + d.b.values.astype(np.int64))


def agrupar(t: pd.DataFrame, o: pd.DataFrame, e: pd.DataFrame, ady: np.ndarray) -> pd.DataFrame:
    """Une las tres detecciones. Pares: todo punto con todo punto a ≤ UNION_M; además, cada cruce por terreno con
    el punto de OSM o de estructura más cercano de su mismo camino o de un camino que lo toca, a ≤ SNAP_M
    (corrimiento del cauce de FABDEM).
    Ubicación del grupo: la estructura (puente antes que alcantarilla/vado), si no la intersección con el cauce
    OSM, si no el pico del terreno. Área drenada: la mayor del grupo."""
    t = t.assign(por_terreno=True, por_osm=False, por_estructura=False, prio=9)
    o = o.assign(por_terreno=False, por_osm=True, por_estructura=False, prio=5)
    e = e.drop(columns=['src']).assign(por_terreno=False, por_osm=False, por_estructura=True)
    a = pd.concat([t, o, e], ignore_index=True)
    x, y = A_UTM.transform(a.lon.values, a.lat.values)
    xy = np.column_stack([x, y])
    pares = [cKDTree(xy).query_pairs(UNION_M, output_type='ndarray')]
    it = np.flatnonzero(a.por_terreno.values)
    io_ = np.flatnonzero(~a.por_terreno.values)
    cam = a.camino_id.values.astype(np.int64)

    def anclar(desde: np.ndarray, hacia: np.ndarray) -> np.ndarray:
        """Para cada punto de `desde`, el de `hacia` más cercano a ≤ SNAP_M en su camino o en uno que lo toca."""
        dist, j = cKDTree(xy[hacia]).query(xy[desde], k=8, distance_upper_bound=SNAP_M)
        elegido = np.full(len(desde), -1)
        for col in range(j.shape[1]):
            ok = np.isfinite(dist[:, col]) & (elegido < 0)
            jj = np.where(np.isfinite(dist[:, col]), j[:, col], 0)
            ca, cb = cam[hacia[jj]], cam[desde]
            clave = np.minimum(ca, cb) * 10_000_000 + np.maximum(ca, cb)
            ok &= (ca == cb) | np.isin(clave, ady)
            elegido[ok] = hacia[jj[ok]]
        sel = elegido >= 0
        return np.column_stack([desde[sel], elegido[sel]])

    # terreno → OSM/estructura más cercana; y la estructura u OSM que no tiene terreno a ≤ UNION_M → pico más
    # cercano (si no, un puente queda huérfano cuando el pico ya se ancló a la intersección con el cauce OSM)
    pares.append(anclar(it, io_))
    d_t, _ = cKDTree(xy[it]).query(xy[io_], k=1, distance_upper_bound=UNION_M)
    huerfanos = io_[~np.isfinite(d_t)]
    pares.append(anclar(huerfanos, it))
    pares = np.concatenate(pares)
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    m = coo_matrix((np.ones(len(pares)), (pares[:, 0], pares[:, 1])), shape=(len(a), len(a)))
    _, grupo = connected_components(m, directed=False)
    a['grupo'] = grupo
    a['x'], a['y'] = xy[:, 0], xy[:, 1]
    g = a.groupby('grupo')
    rep = a.sort_values(['grupo', 'prio', 'area_drenada_km2'], ascending=[True, True, False]).groupby('grupo').first()
    pico = a[a.por_terreno].sort_values('area_drenada_km2', ascending=False).groupby('grupo').first()
    rep['area_drenada_km2'] = g.area_drenada_km2.max()
    rep['por_terreno'] = g.por_terreno.any()
    rep['por_osm'] = g.por_osm.any()
    rep['por_estructura'] = g.por_estructura.any()
    rep['racha_m'] = g.racha_m.max()
    rep['desplazamiento_m'] = np.hypot(rep.x - pico.x.reindex(rep.index), rep.y - pico.y.reindex(rep.index))
    return rep.reset_index(drop=True)


EVIDENCIA = """
CREATE TEMP TABLE _puentes_osm AS SELECT geom FROM vial.osm o
  WHERE o.provincia = 'misiones' AND o.tags ? 'bridge' AND o.tags->>'bridge' <> 'no';
CREATE INDEX ON _puentes_osm USING gist (geom);
UPDATE infra.cruces c SET
  puente_osm = EXISTS (SELECT 1 FROM _puentes_osm o WHERE ST_DWithin(o.geom, c.geom, 0.0006) AND ST_DWithin(o.geom::geography, c.geom::geography, 50)),
  puente_visor = EXISTS (SELECT 1 FROM infra.visor_puentes v WHERE ST_DWithin(v.geom, c.geom, 0.0006) AND ST_DWithin(v.geom::geography, c.geom::geography, 50)),
  alcantarilla_osm = EXISTS (SELECT 1 FROM infra.osm_estructuras e WHERE e.tipo = 'alcantarilla'
                             AND ST_DWithin(e.geom, c.geom, 0.0006) AND ST_DWithin(e.geom::geography, c.geom::geography, 50)),
  alcantarilla_visor = EXISTS (SELECT 1 FROM infra.visor_alcantarillas v WHERE ST_DWithin(v.geom, c.geom, 0.0006) AND ST_DWithin(v.geom::geography, c.geom::geography, 50)),
  vado_osm = EXISTS (SELECT 1 FROM infra.osm_estructuras e WHERE e.tipo = 'vado'
                     AND ST_DWithin(e.geom, c.geom, 0.0006) AND ST_DWithin(e.geom::geography, c.geom::geography, 50));
UPDATE infra.cruces c SET (puente_rp2_gid, cota_seguridad_msnm) = (
  SELECT v.gid, v.cota_seguridad_msnm FROM infra.visor_puentes_rp2 v
  WHERE ST_DWithin(v.geom, c.geom, 0.0012) AND ST_DWithin(v.geom::geography, c.geom::geography, 100)
  ORDER BY v.geom::geography <-> c.geom::geography LIMIT 1);
UPDATE infra.cruces SET
  n_fuentes_puente = (puente_osm OR puente_visor)::int + (puente_rp2_gid IS NOT NULL)::int,
  estructura = CASE WHEN puente_osm OR puente_visor OR puente_rp2_gid IS NOT NULL THEN 'puente'
                    WHEN vado_osm THEN 'vado'
                    WHEN alcantarilla_osm OR alcantarilla_visor THEN 'alcantarilla'
                    ELSE 'sin_dato' END;
UPDATE infra.cruces c SET (cauce_nombre, cauce_tipo) = (
  SELECT w.nombre, w.waterway FROM infra.osm_cauces w
  WHERE ST_DWithin(w.geom, c.geom, 0.0007) AND ST_DWithin(w.geom::geography, c.geom::geography, 60)
  ORDER BY (w.nombre IS NULL), w.geom::geography <-> c.geom::geography LIMIT 1);
UPDATE infra.cruces c SET cauce_nombre = (
  SELECT v.nombre FROM infra.visor_cauces v WHERE v.nombre IS NOT NULL
  AND ST_DWithin(v.geom, c.geom, 0.0007) AND ST_DWithin(v.geom::geography, c.geom::geography, 60)
  ORDER BY v.geom::geography <-> c.geom::geography LIMIT 1)
WHERE c.cauce_nombre IS NULL;
UPDATE infra.cruces SET rio_frontera = coalesce(cauce_tipo = 'river'
  AND cauce_nombre ~* '(paran[aá]|uruguay|iguaz|pepir|san antonio)', false);
UPDATE infra.cruces c SET municipio = m.nombre
FROM infra.visor_municipios m WHERE ST_Intersects(m.geom, c.geom);
UPDATE infra.cruces SET h3 = h3_lat_lng_to_cell(geom::point, 9);
"""


def main() -> int:
    t0 = time.time()
    acc, tr = leer_acc()
    log(f'área drenada leída ({time.time() - t0:.0f} s)')
    conn = connect(DB)
    cam = caminos(conn)
    t = por_terreno(cam, acc, tr)
    log(f'terreno: {len(t):,} cruces ({time.time() - t0:.0f} s)')
    o = por_osm(conn, acc, tr)
    log(f'OSM: {len(o):,} intersecciones camino × cauce ({time.time() - t0:.0f} s)')
    e = por_estructura(conn, acc, tr)
    log(f'estructuras sobre caminos: {len(e):,} ({time.time() - t0:.0f} s)')
    ady = adyacencia(conn)
    log(f'pares de caminos que se tocan: {len(ady):,} ({time.time() - t0:.0f} s)')
    c = agrupar(t, o, e, ady)
    c = c.merge(cam[['id', 'clase', 'superficie', 'nombre', 'ref']].rename(columns={
        'id': 'camino_id', 'clase': 'clase_camino', 'nombre': 'nombre_camino', 'ref': 'ref_camino'}), on='camino_id')
    c['paralelo'] = c.racha_m > PARALELO_M
    log(f'cruces unidos: {len(c):,} ({time.time() - t0:.0f} s)')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.cruces')
    cur.execute("""CREATE TABLE infra.cruces (
        id serial PRIMARY KEY, camino_id integer, clase_camino text, superficie text, nombre_camino text,
        ref_camino text, area_drenada_km2 real, racha_m real, paralelo boolean, por_terreno boolean,
        por_osm boolean, por_estructura boolean, desplazamiento_m real, cauce_nombre text, cauce_tipo text, rio_frontera boolean DEFAULT false,
        puente_osm boolean, puente_visor boolean, puente_rp2_gid integer, cota_seguridad_msnm double precision,
        alcantarilla_osm boolean, alcantarilla_visor boolean, vado_osm boolean, n_fuentes_puente smallint,
        estructura text, municipio text, h3 h3index, geom geometry(Point, 4326))""")
    cols = ['camino_id', 'clase_camino', 'superficie', 'nombre_camino', 'ref_camino', 'area_drenada_km2',
            'racha_m', 'paralelo', 'por_terreno', 'por_osm', 'por_estructura', 'desplazamiento_m', 'lon', 'lat']
    buf = io.StringIO()
    c[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.execute('CREATE TEMP TABLE _c (camino_id int, clase_camino text, superficie text, nombre_camino text, '
                'ref_camino text, area_drenada_km2 real, racha_m real, paralelo boolean, por_terreno boolean, '
                'por_osm boolean, por_estructura boolean, desplazamiento_m real, lon double precision, lat double precision)')
    cur.copy_expert("COPY _c FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute(f"""INSERT INTO infra.cruces ({', '.join(cols[:-2])}, geom)
                    SELECT {', '.join(cols[:-2])}, ST_SetSRID(ST_MakePoint(lon, lat), 4326) FROM _c""")
    cur.execute('CREATE INDEX ON infra.cruces USING gist (geom)')
    cur.execute('DROP TABLE IF EXISTS infra.cruces_terreno')
    cur.execute('CREATE TABLE infra.cruces_terreno (camino_id int, area_drenada_km2 real, racha_m real, '
                'lon double precision, lat double precision)')
    buf2 = io.StringIO()
    t[['camino_id', 'area_drenada_km2', 'racha_m', 'lon', 'lat']].to_csv(buf2, index=False, header=False)
    buf2.seek(0)
    cur.copy_expert('COPY infra.cruces_terreno FROM STDIN WITH (FORMAT csv)', buf2)
    cur.execute('ALTER TABLE infra.cruces_terreno ADD COLUMN geom geometry(Point, 4326)')
    cur.execute('UPDATE infra.cruces_terreno SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)')
    cur.execute('CREATE INDEX ON infra.cruces_terreno USING gist (geom)')
    cur.execute("COMMENT ON TABLE infra.cruces_terreno IS 'Picos de área drenada a lo largo de cada camino, "
                "antes de unir con OSM (diagnóstico de cruces.py)'")
    conn.commit()
    for paso in [s for s in EVIDENCIA.split(';\n') if s.strip()]:
        cur.execute(paso)
        conn.commit()
    log(f'evidencia y atributos ({time.time() - t0:.0f} s)')
    cur.execute("COMMENT ON TABLE infra.cruces IS 'Cruces camino × cauce de Misiones (pipeline/infra/cruces.py): "
                "detección por área drenada FABDEM 30 m (≥ 0,1 km²) y por intersección con cauces OSM; evidencia "
                "de puente, vado o alcantarilla de OSM y del visor de emergencias de Misiones.'")
    conn.commit()
    cur.execute('SELECT estructura, count(*) FROM infra.cruces GROUP BY 1 ORDER BY 2 DESC')
    log('  por estructura:', dict(cur.fetchall()))
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
