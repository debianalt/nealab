"""Velocidad y latencia de internet móvil y fijo en Misiones: Ookla Open Data, todos los trimestres (fase 4: sólo datos).

Fuente: s3://ookla-open-data/parquet/performance/type={mobile,fixed}/year=AAAA/quarter=Q/ (público, sin clave).
Cada fila es una tesela web-mercator de zoom 16 (~610 m en el ecuador, ~540 m en Misiones) con promedios del
trimestre de las pruebas Speedtest: bajada y subida (kbps), latencia (ms), pruebas y dispositivos. Una tesela sin
pruebas no aparece: ausencia ≠ sin señal.

Lectura con DuckDB httpfs filtrando por rango de quadkey (zoom 9) de Misiones: sólo se bajan los grupos de filas
de la zona (~8 s por archivo). Copia local por trimestre en %HEXOMA_FUENTES%/infra/ookla/.

  infra.ookla   tipo, anio, trimestre, quadkey, lon/lat del centro, h3 res-9 del centro, avg_d_kbps, avg_u_kbps,
                avg_lat_ms, tests, devices (recortado a Misiones + 5 km)

ENACOM (conectividad por localidad) no tiene descarga directa hoy (7-nov-2026: geoserver sin respuesta, páginas de
datos abiertos sólo con mapas): queda fuera.

Uso (desde neahub/):
    python pipeline/infra/conectividad.py [--rebajar]
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import math
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from drenaje import BBOX  # noqa: E402

DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'ookla'
BASE = 'https://ookla-open-data.s3.amazonaws.com/parquet/performance'
Z_FILTRO = 9


def tesela(lon: float, lat: float, z: int) -> tuple[int, int]:
    n = 2 ** z
    x = int((lon + 180) / 360 * n)
    y = int((1 - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))) / math.pi) / 2 * n)
    return x, y


def quadkey(x: int, y: int, z: int) -> str:
    s = ''
    for i in range(z, 0, -1):
        m = 1 << (i - 1)
        s += str((1 if x & m else 0) + (2 if y & m else 0))
    return s


def centro_quadkey(qk: str) -> tuple[float, float]:
    x = y = 0
    for ch in qk:
        d = int(ch)
        x, y = x * 2 + (d & 1), y * 2 + (d >> 1)
    n = 2 ** len(qk)
    lon = (x + 0.5) / n * 360 - 180
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 0.5) / n))))
    return lon, lat


def condicion() -> str:
    x0, y0 = tesela(BBOX[0], BBOX[3], Z_FILTRO)
    x1, y1 = tesela(BBOX[2], BBOX[1], Z_FILTRO)
    pref = sorted({quadkey(x, y, Z_FILTRO) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)})
    return ' OR '.join(f"(quadkey >= '{p}' AND quadkey < '{p[:-1]}{int(p[-1]) + 1}')" for p in pref)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    a = ap.parse_args()
    DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    cond = condicion()
    hoy = dt.date.today()
    partes = []
    for tipo in ('mobile', 'fixed'):
        for anio in range(2019, hoy.year + 1):
            for q in range(1, 5):
                mes = 3 * (q - 1) + 1
                if dt.date(anio, mes, 1) > hoy:
                    continue
                local = DIR / f'{tipo}_{anio}_q{q}.parquet'
                if not local.exists() or a.rebajar:
                    url = f'{BASE}/type={tipo}/year={anio}/quarter={q}/{anio}-{mes:02d}-01_performance_{tipo}_tiles.parquet'
                    try:
                        con.execute(f"""COPY (SELECT quadkey, avg_d_kbps, avg_u_kbps, avg_lat_ms, tests, devices
                                              FROM read_parquet('{url}') WHERE {cond})
                                        TO '{local.as_posix()}' (FORMAT parquet)""")
                    except Exception as e:  # noqa: BLE001
                        log(f'  {tipo} {anio} T{q}: no disponible ({str(e)[:70]})')
                        continue
                d = con.execute(f"SELECT * FROM read_parquet('{local.as_posix()}')").df()
                d['tipo'], d['anio'], d['trimestre'] = tipo, anio, q
                partes.append(d)
                log(f'  {tipo} {anio} T{q}: {len(d):,} teselas, {int(d.tests.sum()):,} pruebas')
    df = pd.concat(partes, ignore_index=True)
    c = df.quadkey.map(centro_quadkey)
    df['lon'], df['lat'] = [p[0] for p in c], [p[1] for p in c]
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.ookla')
    cur.execute("""CREATE TABLE infra.ookla (tipo text, anio int, trimestre int, quadkey text, lon double precision,
                   lat double precision, avg_d_kbps bigint, avg_u_kbps bigint, avg_lat_ms bigint, tests bigint,
                   devices bigint)""")
    buf = io.StringIO()
    df[['tipo', 'anio', 'trimestre', 'quadkey', 'lon', 'lat', 'avg_d_kbps', 'avg_u_kbps', 'avg_lat_ms', 'tests',
        'devices']].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY infra.ookla FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('ALTER TABLE infra.ookla ADD COLUMN geom geometry(Point, 4326), ADD COLUMN h3 h3index')
    cur.execute('UPDATE infra.ookla SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)')
    cur.execute("""DELETE FROM infra.ookla o WHERE NOT EXISTS (SELECT 1 FROM limites l WHERE l.territorio_id = 1
                   AND ST_DWithin(l.geom, o.geom, 0.05))""")
    cur.execute('UPDATE infra.ookla SET h3 = h3_lat_lng_to_cell(geom::point, 9)')
    cur.execute('CREATE INDEX ON infra.ookla USING gist (geom)')
    cur.execute('CREATE INDEX ON infra.ookla (tipo, anio, trimestre)')
    cur.execute("COMMENT ON TABLE infra.ookla IS 'Ookla Open Data (Speedtest), teselas de zoom 16 con promedios del "
                "trimestre, móvil y fija, 2019–; recortado a Misiones + 5 km (conectividad.py). Ausencia de tesela = sin "
                "pruebas, no sin señal.'")
    cur.execute('SELECT count(*) FROM infra.ookla')
    n = cur.fetchone()[0]
    cur.execute("""INSERT INTO infra.fuentes VALUES ('infra.ookla', 'Ookla Open Data (s3 ookla-open-data)', %s,
                   %s, now(), %s, 'performance tiles, mobile y fixed') ON CONFLICT (tabla) DO UPDATE SET
                   fecha_fuente=EXCLUDED.fecha_fuente, cargado=now(), n=EXCLUDED.n""",
                (BASE, f'{df.anio.max()}-T{df[df.anio == df.anio.max()].trimestre.max()}', n))
    conn.commit()
    conn.close()
    log(f'infra.ookla: {n:,} filas')
    return 0


if __name__ == '__main__':
    sys.exit(main())
