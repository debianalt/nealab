"""Red de arroyos de Misiones como tabla: tramos del D8 de FABDEM con área drenada, orden, pendiente y nombre (fase 5).

Desde los rásteres de drenaje.py (fdir y acc a 30 m): la red son las celdas con ≥ A_RED km² drenados (0,1 km², el
umbral de la demo El Soberbio y de cruces.py). pysheds:
  extract_river_network(fdir, red)  tramos entre confluencias (líneas en lon/lat)
  stream_order(fdir, red)           orden de Strahler

  infra.arroyos   arroyo_id, orden, area_arriba_km2, area_abajo_km2, largo_m, z_inicio, z_fin, pendiente_pct,
                  nombre (IGN, si no OSM, a ≤ 100 m del punto medio), perenne (IGN: true/false; NULL sin dato), geom
  infra.cruces    + arroyo_id (tramo más cercano a ≤ 60 m)

Los ríos de frontera (Paraná, Uruguay, Iguazú, Pepirí Guazú, San Antonio) tienen el área truncada en el borde del
recorte (ver drenaje.py).

Uso (desde neahub/, después de drenaje.py, cauces_osm.py, hidro_ign.py y cruces.py):
    python pipeline/infra/red_arroyos.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402  (limpia PROJ_LIB)
from build_hexoma import DB, connect, log  # noqa: E402

A_RED = 0.1


def main() -> int:
    import rasterio
    import shapely
    from pyproj import Transformer
    from pysheds.grid import Grid
    t0 = time.time()
    p_fdir = drenaje.FUENTES / 'fdir_misiones.tif'
    grid = Grid.from_raster(str(p_fdir))
    fdir = grid.read_raster(str(p_fdir)).astype(np.int64)
    acc = grid.read_raster(str(drenaje.FUENTES / 'acc_km2_misiones.tif'))
    red = acc >= A_RED
    log(f'rásteres leídos; celdas de red {int(np.asarray(red).sum()):,} ({time.time() - t0:.0f} s)')
    orden = np.asarray(grid.stream_order(fdir, red))
    log(f'orden de Strahler: máx {orden.max()} ({time.time() - t0:.0f} s)')
    fc = grid.extract_river_network(fdir, red)
    feats = fc['features'] if isinstance(fc, dict) else fc.features
    log(f'tramos: {len(feats):,} ({time.time() - t0:.0f} s)')
    acc = np.asarray(acc)
    with rasterio.open(drenaje.FUENTES / 'fabdem_misiones.tif') as src:
        dem = src.read(1)
        tr = src.transform

    def idx(lon, lat):
        c = np.clip(np.floor((np.asarray(lon) - tr.c) / tr.a).astype(int), 0, dem.shape[1] - 1)
        f = np.clip(np.floor((np.asarray(lat) - tr.f) / tr.e).astype(int), 0, dem.shape[0] - 1)
        return f, c

    utm = Transformer.from_crs('EPSG:4326', 'EPSG:32721', always_xy=True)
    filas = []
    for k, ft in enumerate(feats):
        g = ft['geometry'] if isinstance(ft, dict) else ft.geometry
        co = np.asarray(g['coordinates'] if isinstance(g, dict) else g.coordinates, dtype=float)
        if len(co) < 2:
            continue
        f, c = idx(co[:, 0], co[:, 1])
        a = acc[f, c]
        x, y = utm.transform(co[:, 0], co[:, 1])
        largo = float(np.hypot(np.diff(x), np.diff(y)).sum())
        z0, z1 = float(dem[f[0], c[0]]), float(dem[f[-1], c[-1]])
        filas.append((k, int(orden[f, c].max()), float(a.min()), float(a.max()), largo, z0, z1,
                      (z0 - z1) / largo * 100 if largo > 0 else np.nan,
                      'LINESTRING(' + ','.join(f'{p[0]:.6f} {p[1]:.6f}' for p in co) + ')'))
    df = pd.DataFrame(filas, columns=['arroyo_id', 'orden', 'area_arriba_km2', 'area_abajo_km2', 'largo_m', 'z_inicio',
                                      'z_fin', 'pendiente_pct', 'wkt'])
    # pysheds devuelve colas fuera de la red (orden 0, < 0,1 km² drenados; 26 mil tramos el 7-nov-2026): afuera
    n0 = int((df.orden == 0).sum())
    df = df[df.orden > 0]
    log(f'tramos de orden 0 descartados (fuera de la red): {n0:,}')
    log(f'atributos: {len(df):,} tramos, {df.largo_m.sum() / 1000:,.0f} km ({time.time() - t0:.0f} s)')
    del dem, acc, orden, fdir

    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.arroyos CASCADE')
    cur.execute("""CREATE TABLE infra.arroyos (arroyo_id int PRIMARY KEY, orden smallint, area_arriba_km2 real,
                   area_abajo_km2 real, largo_m real, z_inicio real, z_fin real, pendiente_pct real, wkt text)""")
    buf = io.StringIO()
    df.round(3).to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY infra.arroyos FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""ALTER TABLE infra.arroyos ADD COLUMN geom geometry(LineString, 4326), ADD COLUMN nombre text,
                   ADD COLUMN perenne boolean""")
    cur.execute('UPDATE infra.arroyos SET geom = ST_GeomFromText(wkt, 4326)')
    cur.execute('ALTER TABLE infra.arroyos DROP COLUMN wkt')
    cur.execute('CREATE INDEX ON infra.arroyos USING gist (geom)')
    conn.commit()
    log(f'cargado ({time.time() - t0:.0f} s)')
    # nombre y régimen: IGN (perenne/intermitente) y, si no, OSM; a ≤ 100 m del punto medio del tramo
    cur.execute("""CREATE TEMP TABLE _ign AS
                   SELECT nombre, true AS perenne, geom FROM infra.ign_cursos_perennes
                   UNION ALL SELECT nombre, false, geom FROM infra.ign_cursos_intermitentes""")
    cur.execute('CREATE INDEX ON _ign USING gist (geom)')
    cur.execute("""UPDATE infra.arroyos a SET (nombre, perenne) = (
                     SELECT i.nombre, i.perenne FROM _ign i
                     WHERE ST_DWithin(i.geom, ST_LineInterpolatePoint(a.geom, 0.5), 0.0011)
                     ORDER BY i.geom <-> ST_LineInterpolatePoint(a.geom, 0.5) LIMIT 1)""")
    cur.execute("""UPDATE infra.arroyos a SET nombre = (
                     SELECT w.nombre FROM infra.osm_cauces w WHERE w.nombre IS NOT NULL
                     AND ST_DWithin(w.geom, ST_LineInterpolatePoint(a.geom, 0.5), 0.0011)
                     ORDER BY w.geom <-> ST_LineInterpolatePoint(a.geom, 0.5) LIMIT 1)
                   WHERE a.nombre IS NULL""")
    conn.commit()
    log(f'nombres ({time.time() - t0:.0f} s)')
    # cruces → tramo
    cur.execute('ALTER TABLE infra.cruces DROP COLUMN IF EXISTS arroyo_id')
    cur.execute('ALTER TABLE infra.cruces ADD COLUMN arroyo_id int')
    cur.execute("""UPDATE infra.cruces c SET arroyo_id = (
                     SELECT a.arroyo_id FROM infra.arroyos a WHERE ST_DWithin(a.geom, c.geom, 0.0007)
                     ORDER BY a.geom <-> c.geom LIMIT 1)""")
    cur.execute("""COMMENT ON TABLE infra.arroyos IS 'Red de arroyos de Misiones: tramos entre confluencias del D8 de FABDEM
                   30 m con ≥ 0,1 km² drenados (pysheds); orden de Strahler; nombre y régimen del IGN (si no, OSM) a ≤ 100 m
                   (red_arroyos.py)'""")
    cur.execute("""INSERT INTO infra.fuentes VALUES ('infra.arroyos', 'Derivado: FABDEM 30 m + pysheds D8', 'drenaje.py',
                   current_date::text, now(), %s, 'umbral 0,1 km²') ON CONFLICT (tabla) DO UPDATE SET cargado=now(),
                   n=EXCLUDED.n""", (len(df),))
    conn.commit()
    cur.execute("""SELECT count(*) FILTER (WHERE nombre IS NOT NULL), count(*) FILTER (WHERE perenne IS NOT NULL),
                          (SELECT count(*) FILTER (WHERE arroyo_id IS NOT NULL) FROM infra.cruces), (SELECT count(*) FROM infra.cruces)
                   FROM infra.arroyos""")
    n_nom, n_per, c_ok, c_tot = cur.fetchone()
    log(f'listo en {time.time() - t0:.0f} s: con nombre {n_nom:,}; con régimen IGN {n_per:,}; cruces ligados {c_ok:,}/{c_tot:,}')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
