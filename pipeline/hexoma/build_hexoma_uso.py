"""Hexoma A2 — uso y cobertura del suelo por celda y año, 1985–2025 (MapBiomas Argentina, Colección 3).

Fuente: projects/mapbiomas-public/assets/argentina/lulc/collection3/mapbiomas_argentina_collection3_coverage_v1
(bandas classification_1985 … classification_2025, 19 clases, ~30 m, EPSG:4326). La leyenda sale de los
metadatos del asset (en inglés); los rótulos en castellano son traducción propia.

Método:
1. Bloques de 4096 × 4096 píxeles en la grilla nativa, sólo los que tienen algún centro de celda y sus
   vecinos (para no cortar celdas de borde). Descarga con ee.data.computePixels, un año por pedido (uint8).
2. Cada píxel va a la celda H3 res-9 que contiene su centro (DuckDB h3, una vez por bloque); ~130 píxeles por
   celda. Por año, conteo de píxeles por clase y celda (bincount). Los rásters no se guardan: se regeneran.
3. Salida:
   %HEXOMA_FUENTES%/uso/uso_h3_anio_clase.parquet   serie completa: (h3, anio, clase, n_px), sin ceros
   uso.clases            leyenda de la Col. 3 y grupo de cada clase
   uso.h3_anio           ‰ de la superficie de la celda por grupo, años 1985, 1990, …, 2020 y 2025 (regla de
                         tamaño del plan: la serie completa pasaba de 6 GB en Postgres)
   uso.h3_trayectoria    por celda: grupo dominante 1985 y 2025, % de bosque y de agricultura en 1985, 2000 y
                         2025, primer año agrícola, año de pérdida de bosque, cambios de grupo dominante
   uso.v_serie(h3)       función: la serie anual completa de una celda leída del parquet (DuckDB no está en
                         Postgres: la serie completa se consulta con DuckDB sobre el parquet)

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_uso.py               # baja, agrega y carga
    python pipeline/hexoma/build_hexoma_uso.py --solo-carga  # sólo carga a Postgres desde el parquet
"""
from __future__ import annotations

import argparse
import io
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
USO = FUENTES_DIR / 'uso'
CENTROS = FUENTES_DIR / 'clima' / 'centros.parquet'
ASSET = 'projects/mapbiomas-public/assets/argentina/lulc/collection3/mapbiomas_argentina_collection3_coverage_v1'
ANIOS = list(range(1985, 2026))
ANIOS_PG = [1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2025]
BLOQUE = 4096

# código → (rótulo del asset, rótulo en castellano, grupo)
CLASES = {
    3: ('Closed forests', 'Bosque cerrado', 'bosque'),
    4: ('Open forests', 'Bosque abierto', 'bosque'),
    6: ('Flooded forests', 'Bosque inundable', 'bosque'),
    63: ('Shrub and herbaceous mosaics', 'Mosaico de arbustal y herbáceas', 'arbustal'),
    66: ('Closed shrublands', 'Arbustal cerrado', 'arbustal'),
    77: ('Open shrublands', 'Arbustal abierto', 'arbustal'),
    11: ('Flooded grassland', 'Pastizal inundable', 'pastizal'),
    12: ('Grassland', 'Pastizal', 'pastizal'),
    73: ('Peatlands', 'Turbera', 'pastizal'),
    34: ('Ice and permanent snow', 'Hielo y nieve', 'otro'),
    9: ('Forest plantations', 'Plantación forestal', 'plantacion'),
    15: ('Pastures', 'Pastura', 'pastura'),
    19: ('Temporary crops', 'Cultivo temporal', 'cultivo'),
    21: ('Agriculture and pasture mosaic', 'Mosaico de agricultura y pastura', 'mosaico_agro'),
    24: ('Urban areas', 'Área urbana', 'urbano'),
    36: ('Perennial crops', 'Cultivo perenne', 'cultivo_perenne'),
    25: ('Other non-vegetated areas', 'Otra área sin vegetación', 'otro'),
    33: ('Rivers, lakes or ocean', 'Río, lago u océano', 'agua'),
    27: ('Not observed', 'No observado', 'no_observado'),
}
GRUPOS = ['bosque', 'arbustal', 'pastizal', 'plantacion', 'pastura', 'cultivo', 'mosaico_agro', 'cultivo_perenne',
          'urbano', 'agua', 'otro', 'no_observado']
CODIGOS = sorted(CLASES)
IDX = np.full(256, -1, np.int16)
for i, c in enumerate(CODIGOS):
    IDX[c] = i


def grilla():
    import ee
    p = ee.Image(ASSET).projection().getInfo()
    dx, _, x0, _, dy, y0 = p['transform']
    return dx, x0, dy, y0


def bloques(dx, x0, dy, y0, cen):
    bc = np.floor((cen.lng.values - x0) / dx / BLOQUE).astype(int)
    bf = np.floor((cen.lat.values - y0) / dy / BLOQUE).astype(int)
    s = set(zip(bf.tolist(), bc.tolist()))
    vec = {(f + a, c + b) for f, c in s for a in (-1, 0, 1) for b in (-1, 0, 1)}
    return sorted(vec)


def bajar_bloque(img, g, bf, bc, anios):
    import ee
    dx, x0, dy, y0 = g
    req = {'expression': img.select([f'classification_{a}' for a in anios]).toUint8(), 'fileFormat': 'NPY',
           'grid': {'dimensions': {'width': BLOQUE, 'height': BLOQUE},
                    'affineTransform': {'scaleX': dx, 'shearX': 0, 'translateX': x0 + bc * BLOQUE * dx,
                                        'shearY': 0, 'scaleY': dy, 'translateY': y0 + bf * BLOQUE * dy},
                    'crsCode': 'EPSG:4326'}}
    for intento in range(5):
        try:
            r = np.load(io.BytesIO(ee.data.computePixels(req)), max_header_size=10 ** 7)
            return {a: r[f'classification_{a}'] for a in anios}
        except Exception as e:
            if intento == 4:
                raise
            time.sleep(10 * (intento + 1))


def procesar(args) -> None:
    import duckdb
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    cen = pd.read_parquet(CENTROS)
    g = grilla()
    bl = bloques(*g, cen)
    log(f'{len(bl)} bloques de {BLOQUE}² píxeles; {len(cen):,} celdas')
    con = duckdb.connect()
    con.execute('LOAD h3')
    con.register('cen_df', pd.DataFrame({'h3': cen.h3.values, 'i': np.arange(len(cen), dtype=np.int32)}))
    con.execute('CREATE TABLE cen AS SELECT h3_string_to_h3(h3) AS c, i FROM cen_df')
    # uint8 alcanza: una celda res-9 tiene ~110–150 píxeles de 30 m en total (se controla abajo). 2,6 GB en memoria.
    # Reanudable: cada CADA bloques se guardan la cuenta parcial y la lista de bloques hechos.
    USO.mkdir(parents=True, exist_ok=True)
    parcial, hechos = USO / 'cuenta_parcial.npy', USO / 'bloques_hechos.txt'
    if parcial.exists() and hechos.exists() and not args.max_bloques:
        cuenta = np.load(parcial)
        listos = {tuple(map(int, l.split(','))) for l in hechos.read_text().split()}
        log(f'reanudo: {len(listos)} bloques ya hechos')
    else:
        cuenta = np.zeros((len(cen), len(ANIOS), len(CODIGOS)), dtype=np.uint8)
        listos = set()
    img = ee.Image(ASSET)
    dx, x0, dy, y0 = g
    t0 = time.time()
    pares = [[a] for a in ANIOS]  # un año por pedido: el límite cuenta ~2 B por píxel y banda (dato + máscara)
    if args.max_bloques:
        bl = bl[len(bl) // 2: len(bl) // 2 + args.max_bloques]
    CADA = 5
    for n, (bf, bc) in enumerate(bl):
        if (bf, bc) in listos:
            continue
        # índice píxel → celda del bloque
        ff, cc = np.mgrid[0:BLOQUE, 0:BLOQUE]
        lng = (x0 + (bc * BLOQUE + cc.ravel() + 0.5) * dx)
        lat = (y0 + (bf * BLOQUE + ff.ravel() + 0.5) * dy)
        del ff, cc
        con.register('px', pd.DataFrame({'k': np.arange(len(lat), dtype=np.int64), 'lat': lat, 'lng': lng}))
        del lat, lng
        idx = con.execute("""SELECT coalesce(cen.i, -1) AS i FROM (SELECT k, h3_latlng_to_cell(lat, lng, 9) AS c FROM px) p
                             LEFT JOIN cen USING (c) ORDER BY k""").fetchnumpy()
        con.unregister('px')
        celda = idx['i'].astype(np.int64)
        del idx
        dentro = celda >= 0
        if dentro.any():
            u, cel = np.unique(celda[dentro], return_inverse=True)
            del celda
            # de a 4 pedidos y se procesa cada año apenas llega: no se juntan los 41 años en memoria
            with ThreadPoolExecutor(4) as ex:
                for r in ex.map(lambda an: bajar_bloque(img, g, bf, bc, an), pares):
                    for a, arr in r.items():
                        cls = IDX[arr.ravel()[dentro]]
                        ok = cls >= 0
                        k = cel[ok] * len(CODIGOS) + cls[ok]
                        bc_ = np.bincount(k, minlength=len(u) * len(CODIGOS)).reshape(len(u), len(CODIGOS))
                        nuevo = cuenta[u, ANIOS.index(a), :].astype(np.uint16) + bc_
                        assert nuevo.max() <= 255, 'más de 255 píxeles de una clase en una celda'
                        cuenta[u, ANIOS.index(a), :] = nuevo.astype(np.uint8)
            log(f'  bloque {n + 1}/{len(bl)} ({bf},{bc}): {dentro.sum():,} píxeles en celdas ({time.time() - t0:.0f} s)')
        listos.add((bf, bc))
        if not args.max_bloques and (len(listos) % CADA == 0 or n == len(bl) - 1):
            np.save(parcial, cuenta)
            hechos.write_text('\n'.join(f'{a},{b}' for a, b in sorted(listos)))
    if args.max_bloques:
        log(f'prueba: {int(cuenta.sum(dtype=np.int64)):,} píxeles contados; celdas con dato {int((cuenta.sum(axis=(1, 2), dtype=np.int64) > 0).sum()):,}')
        return
    np.save(USO / 'cuenta.npy', cuenta)
    escribir_parquet(cen, cuenta)


def escribir_parquet(cen, cuenta) -> None:
    """Serie larga sin ceros (h3, anio, clase, n_px), un año por vez; se escribe a .tmp y se renombra al cerrar."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    destino = USO / 'uso_h3_anio_clase.parquet'
    tmp = destino.with_suffix('.tmp.parquet')
    w = None
    for j, a in enumerate(ANIOS):
        c = np.asarray(cuenta[:, j, :])
        ci, ki = np.nonzero(c)
        t = pa.table({'h3': pa.array(cen.h3.values[ci]), 'anio': pa.array(np.full(len(ci), a, np.int16)),
                      'clase': pa.array(np.array(CODIGOS, np.int16)[ki]), 'n_px': pa.array(c[ci, ki].astype(np.int32))})
        if w is None:
            w = pq.ParquetWriter(tmp, t.schema, compression='zstd')
        w.write_table(t)
    w.close()
    tmp.replace(destino)
    log(f'parquet: {destino.stat().st_size / 1e9:.2f} GB')


def cargar() -> None:
    cen = pd.read_parquet(CENTROS)
    cuenta = np.load(USO / 'cuenta.npy')
    grupo_de = np.array([GRUPOS.index(CLASES[c][2]) for c in CODIGOS])
    nc, na = cuenta.shape[0], len(ANIOS)
    # año por año, para no tener los 41 años × 12 grupos en float (eran ~13 GB)
    bos = np.zeros((nc, na), np.float32)
    agro = np.zeros((nc, na), np.float32)
    dom = np.full((nc, na), -1, np.int8)
    totales = np.zeros((nc, na), np.int16)
    i_agro = [GRUPOS.index(x) for x in ('cultivo', 'mosaico_agro', 'cultivo_perenne')]
    csv_pg = USO / 'h3_anio_pg.csv'
    with open(csv_pg, 'w', encoding='utf-8', newline='') as fcsv:
        for j, a in enumerate(ANIOS):
            G = np.zeros((nc, len(GRUPOS)), np.float32)
            for k, gi in enumerate(grupo_de):
                G[:, gi] += cuenta[:, j, k]
            tot = G.sum(axis=1)
            totales[:, j] = tot
            with np.errstate(invalid='ignore', divide='ignore'):
                frac = G / tot[:, None]
            bos[:, j] = frac[:, GRUPOS.index('bosque')]
            agro[:, j] = frac[:, i_agro].sum(axis=1)
            dom[:, j] = np.where(tot > 0, G.argmax(axis=1), -1)
            if a in ANIOS_PG:
                hay = tot > 0
                d = pd.DataFrame(np.round(frac[hay] * 1000).astype(np.int16), columns=GRUPOS)
                d.insert(0, 'anio', a)
                d.insert(0, 'h3', cen.h3.values[hay])
                d['n_px'] = tot[hay].astype(np.int32)
                d.to_csv(fcsv, index=False, header=False)
            del G, frac
    tot = totales[:, :, None]
    ia = {a: ANIOS.index(a) for a in ANIOS}

    def primer(cond):
        tiene = cond.any(axis=1)
        return np.where(tiene, np.array(ANIOS)[cond.argmax(axis=1)], -1)
    era_bosque = bos[:, ia[1985]] >= 0.5
    perdida = primer((bos < 0.5) & era_bosque[:, None])
    tray = pd.DataFrame({
        'h3': cen.h3.values, 'n_px_2025': tot[:, ia[2025], 0].astype(np.int32),
        'dominante_1985': [GRUPOS[i] if i >= 0 else None for i in dom[:, ia[1985]]],
        'dominante_2025': [GRUPOS[i] if i >= 0 else None for i in dom[:, ia[2025]]],
        'bosque_1985': bos[:, ia[1985]] * 100, 'bosque_2000': bos[:, ia[2000]] * 100, 'bosque_2025': bos[:, ia[2025]] * 100,
        'agro_1985': agro[:, ia[1985]] * 100, 'agro_2000': agro[:, ia[2000]] * 100, 'agro_2025': agro[:, ia[2025]] * 100,
        'primer_anio_agro': primer(agro >= 0.5), 'anio_perdida_bosque': perdida,
        'cambios_dominante': (np.diff(dom, axis=1) != 0).sum(axis=1).astype(np.int16),
    })
    for c in ('primer_anio_agro', 'anio_perdida_bosque'):
        tray[c] = tray[c].where(tray[c] > 0).astype('Int16')
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS uso CASCADE')
        cur.execute('CREATE SCHEMA uso')
        cur.execute("COMMENT ON SCHEMA uso IS 'Hexoma A2: uso y cobertura del suelo por celda, MapBiomas Argentina "
                    "Colección 3 (1985–2025, ~30 m). pipeline/hexoma/build_hexoma_uso.py'")
        cur.execute('CREATE TABLE uso.clases (codigo smallint PRIMARY KEY, rotulo_mapbiomas text, rotulo text, grupo text)')
        for c, (en, es, gr) in CLASES.items():
            cur.execute('INSERT INTO uso.clases VALUES (%s, %s, %s, %s)', (c, en, es, gr))
        cur.execute("COMMENT ON TABLE uso.clases IS 'Leyenda de la Colección 3 (metadatos del asset); rótulo en castellano "
                    "por traducción propia; grupo = agregación del Hexoma'")

        def copy(df, tabla):
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, na_rep='')
            buf.seek(0)
            cur.copy_expert(f"COPY {tabla} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        cur.execute('CREATE TEMP TABLE t (h3 text, anio smallint, ' + ', '.join(f'{g} smallint' for g in GRUPOS) + ', n_px int)')
        with open(csv_pg, encoding='utf-8') as fcsv:
            cur.copy_expert(f"COPY t (h3, anio, {', '.join(GRUPOS)}, n_px) FROM STDIN WITH (FORMAT csv)", fcsv)
        cur.execute('CREATE TABLE uso.h3_anio AS SELECT h3::h3index AS h3, anio, ' + ', '.join(GRUPOS) + ', n_px FROM t')
        cur.execute('ALTER TABLE uso.h3_anio ADD PRIMARY KEY (h3, anio)')
        for gname in GRUPOS:
            cs = ', '.join(f'{c} {CLASES[c][1]}' for c in CODIGOS if CLASES[c][2] == gname)
            cur.execute(f"COMMENT ON COLUMN uso.h3_anio.{gname} IS 'superficie de la celda en {gname} ({cs}), ‰ de los píxeles de 30 m con centro en la celda. MapBiomas Argentina Col. 3'")
        cur.execute("COMMENT ON COLUMN uso.h3_anio.n_px IS 'píxeles de 30 m con centro en la celda'")
        cur.execute("COMMENT ON TABLE uso.h3_anio IS 'Uso del suelo por celda en 1985, 1990… 2025 (‰ por grupo). La serie "
                    "anual completa por clase está en %HEXOMA_FUENTES%/uso/uso_h3_anio_clase.parquet (DuckDB)'")
        cur.execute('CREATE TEMP TABLE tt (h3 text, n_px_2025 int, dominante_1985 text, dominante_2025 text, bosque_1985 real, '
                    'bosque_2000 real, bosque_2025 real, agro_1985 real, agro_2000 real, agro_2025 real, '
                    'primer_anio_agro smallint, anio_perdida_bosque smallint, cambios_dominante smallint)')
        copy(tray, 'tt')
        cur.execute('CREATE TABLE uso.h3_trayectoria AS SELECT h3::h3index AS h3, ' + ', '.join(tray.columns[1:]) + ' FROM tt')
        cur.execute('ALTER TABLE uso.h3_trayectoria ADD PRIMARY KEY (h3)')
        for c, d in (('dominante_1985', 'grupo con más superficie en 1985'), ('dominante_2025', 'grupo con más superficie en 2025'),
                     ('bosque_1985', '% de bosque (cerrado, abierto, inundable) en 1985'), ('bosque_2000', '% de bosque en 2000'),
                     ('bosque_2025', '% de bosque en 2025'),
                     ('agro_1985', '% agrícola (cultivo temporal, mosaico agricultura-pastura, cultivo perenne) en 1985'),
                     ('agro_2000', '% agrícola en 2000'), ('agro_2025', '% agrícola en 2025'),
                     ('primer_anio_agro', 'primer año con ≥ 50 % agrícola; NULL = nunca'),
                     ('anio_perdida_bosque', 'primer año con < 50 % de bosque en celdas que en 1985 tenían ≥ 50 %; NULL = no perdió o no era bosque'),
                     ('cambios_dominante', 'veces que cambió el grupo dominante entre años consecutivos 1985–2025 (incluye ruido de clasificación)'),
                     ('n_px_2025', 'píxeles de 30 m con centro en la celda')):
            cur.execute(f"COMMENT ON COLUMN uso.h3_trayectoria.{c} IS '{d}. MapBiomas Argentina Col. 3'")
        cur.execute('ANALYZE uso.h3_anio; ANALYZE uso.h3_trayectoria')
        cur.execute("SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace s "
                    "ON s.oid = c.relnamespace WHERE s.nspname = 'uso' AND c.relkind = 'r'")
        log(f'esquema uso: {cur.fetchone()[0]}')
    verificar(conn)


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""SELECT t.territorio, round(avg(n_px)) px,
                              round(sum(bosque * h3_cell_area(u.h3, 'km^2')) / 10000) AS bosque_kha,
                              round(sum((cultivo + mosaico_agro + cultivo_perenne) * h3_cell_area(u.h3, 'km^2')) / 10000) AS agro_kha,
                              round(sum(plantacion * h3_cell_area(u.h3, 'km^2')) / 10000) AS plant_kha
                       FROM uso.h3_anio u JOIN (SELECT DISTINCT ON (h3) h3, territorio_id FROM hexagonos) x USING (h3)
                       JOIN territorios t USING (territorio_id) WHERE anio = 2025 GROUP BY 1 ORDER BY 1""")
        print('  2025: territorio, píxeles por celda, bosque, agrícola y plantación (miles de ha):')
        for r in cur.fetchall():
            print('   ', r)
        cur.execute("""SELECT anio, round(sum(bosque * h3_cell_area(h3, 'km^2')) / 10000) FROM uso.h3_anio
                       GROUP BY 1 ORDER BY 1""")
        print('  bosque del NEA por año (miles de ha):', cur.fetchall())
        cur.execute("""SELECT count(*) FILTER (WHERE bosque + arbustal + pastizal + plantacion + pastura + cultivo + mosaico_agro
                              + cultivo_perenne + urbano + agua + otro + no_observado NOT BETWEEN 990 AND 1010) FROM uso.h3_anio""")
        print('  filas cuyo ‰ no suma 1000 ± 10:', cur.fetchone()[0])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--solo-carga', action='store_true')
    ap.add_argument('--max-bloques', type=int, help='prueba con N bloques, sin guardar ni cargar')
    ap.add_argument('--solo-parquet', action='store_true', help='rearma el parquet desde cuenta.npy y termina')
    args = ap.parse_args()
    if args.solo_parquet:
        escribir_parquet(pd.read_parquet(CENTROS), np.load(USO / 'cuenta.npy', mmap_mode='r'))
        return 0
    if not args.solo_carga:
        procesar(args)
        if args.max_bloques:
            return 0
    cargar()
    return 0


if __name__ == '__main__':
    sys.exit(main())
