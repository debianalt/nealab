"""Hexoma — serie anual de pérdida de cobertura arbórea por celda, 2001–2025 (Hansen GFC v1.13).

Fuente: UMD/hansen/global_forest_change_2025_v1_13 (Hansen et al. 2013, actualización 2025; la misma versión que la
capa de deforestación y las señales EUDR de la web), bandas treecover2000 (% de dosel en 2000) y lossyear
(0 = sin pérdida, 1–25 = 2001–2025), grilla nativa de 1″ (~30 m), EPSG:4326.

Método (el de build_hexoma_uso.py):
1. Bloques de 4096 × 4096 píxeles de la grilla nativa, sólo los que tienen algún centro de celda y sus vecinos.
   Descarga con ee.data.computePixels, una banda por pedido (uint8).
2. Cada píxel va a la celda H3 res-9 que contiene su centro (DuckDB h3). Por celda: píxeles totales, píxeles con
   dosel 2000 ≥ 10 % y ≥ 30 %, suma del dosel, y píxeles perdidos por año (todos, dentro de dosel ≥ 10 % y ≥ 30 %).
3. Superficie: cada píxel vale área de la celda / píxeles con centro en la celda (corrige la latitud sin fórmula
   aparte). Los umbrales de dosel son los de uso común: 10 % (FAO) y 30 % (Global Forest Watch).
4. Salida:
   %HEXOMA_FUENTES%/hansen/hansen_h3.parquet, hansen_h3_anio.parquet
   series.hansen_h3        por celda: ha, bosque 2000 (≥ 10 y ≥ 30 % de dosel), dosel medio
   series.hansen_h3_anio   por celda y año con pérdida: ha perdidas (todas, en dosel ≥ 10 %, en dosel ≥ 30 %)
   series.v_hansen_tasas   (materializada) por celda: tasas 2001–2010 y 2016–2025 (% de la celda por año, como c_loss_rate de la web)
   y % del bosque 2000 perdido hasta 2025

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_hansen.py                 # baja, agrega y carga (reanudable)
    python pipeline/hexoma/build_hexoma_hansen.py --solo-carga    # carga desde los parquets
    python pipeline/hexoma/build_hexoma_hansen.py --max-bloques 2 # prueba
    python pipeline/hexoma/build_hexoma_hansen.py --validar       # sólo la validación (validacion_hansen.md)
"""
from __future__ import annotations

import argparse
import io
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
HAN = FUENTES_DIR / 'hansen'
CENTROS = FUENTES_DIR / 'clima' / 'centros.parquet'
ASSET = 'UMD/hansen/global_forest_change_2025_v1_13'
ANIOS = list(range(2001, 2026))
BLOQUE = 4096
UMBRALES = (10, 30)
VALIDACION = Path(__file__).with_name('validacion_hansen.md')


def grilla():
    import ee
    p = ee.Image(ASSET).select('lossyear').projection().getInfo()
    dx, _, x0, _, dy, y0 = p['transform']
    return dx, x0, dy, y0


def bloques(dx, x0, dy, y0, cen):
    bc = np.floor((cen.lng.values - x0) / dx / BLOQUE).astype(int)
    bf = np.floor((cen.lat.values - y0) / dy / BLOQUE).astype(int)
    s = set(zip(bf.tolist(), bc.tolist()))
    return sorted({(f + a, c + b) for f, c in s for a in (-1, 0, 1) for b in (-1, 0, 1)})


def bajar(img, g, bf, bc, banda):
    import ee
    dx, x0, dy, y0 = g
    req = {'expression': img.select([banda]).toUint8(), 'fileFormat': 'NPY',
           'grid': {'dimensions': {'width': BLOQUE, 'height': BLOQUE},
                    'affineTransform': {'scaleX': dx, 'shearX': 0, 'translateX': x0 + bc * BLOQUE * dx,
                                        'shearY': 0, 'scaleY': dy, 'translateY': y0 + bf * BLOQUE * dy},
                    'crsCode': 'EPSG:4326'}}
    for intento in range(5):
        try:
            return np.load(io.BytesIO(ee.data.computePixels(req)), max_header_size=10 ** 7)[banda]
        except Exception:
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
    n = len(cen)
    log(f'{len(bl)} bloques de {BLOQUE}² píxeles; {n:,} celdas')
    con = duckdb.connect()
    con.execute('LOAD h3')
    con.register('cen_df', pd.DataFrame({'h3': cen.h3.values, 'i': np.arange(n, dtype=np.int32)}))
    con.execute('CREATE TABLE cen AS SELECT h3_string_to_h3(h3) AS c, i FROM cen_df')
    HAN.mkdir(parents=True, exist_ok=True)
    parcial, hechos = HAN / 'parcial.npz', HAN / 'bloques_hechos.txt'
    if parcial.exists() and hechos.exists() and not args.max_bloques:
        z = np.load(parcial)
        px, bos, dosel, perd = z['px'], z['bos'], z['dosel'], z['perd']
        listos = {tuple(map(int, l.split(','))) for l in hechos.read_text().split()}
        log(f'reanudo: {len(listos)} bloques ya hechos')
    else:
        px = np.zeros(n, np.uint16)                       # píxeles con centro en la celda
        bos = np.zeros((n, len(UMBRALES)), np.uint16)     # píxeles con dosel 2000 ≥ umbral
        dosel = np.zeros(n, np.uint32)                    # suma del dosel 2000 (%)
        perd = np.zeros((n, len(ANIOS), 1 + len(UMBRALES)), np.uint8)  # perdidos: todos, ≥ 10, ≥ 30
        listos = set()
    img = ee.Image(ASSET)
    dx, x0, dy, y0 = g
    t0 = time.time()
    if args.max_bloques:
        bl = bl[len(bl) // 2: len(bl) // 2 + args.max_bloques]
    for k, (bf, bc) in enumerate(bl):
        if (bf, bc) in listos:
            continue
        ff, cc = np.mgrid[0:BLOQUE, 0:BLOQUE]
        lng = x0 + (bc * BLOQUE + cc.ravel() + 0.5) * dx
        lat = y0 + (bf * BLOQUE + ff.ravel() + 0.5) * dy
        del ff, cc
        con.register('pxs', pd.DataFrame({'k': np.arange(len(lat), dtype=np.int64), 'lat': lat, 'lng': lng}))
        del lat, lng
        celda = con.execute("""SELECT coalesce(cen.i, -1) AS i FROM (SELECT k, h3_latlng_to_cell(lat, lng, 9) AS c FROM pxs) p
                               LEFT JOIN cen USING (c) ORDER BY k""").fetchnumpy()['i'].astype(np.int64)
        con.unregister('pxs')
        dentro = celda >= 0
        if dentro.any():
            with ThreadPoolExecutor(2) as ex:
                tc, ly = ex.map(lambda b: bajar(img, g, bf, bc, b), ['treecover2000', 'lossyear'])
            u, cel = np.unique(celda[dentro], return_inverse=True)
            tc = tc.ravel()[dentro].astype(np.int64)
            ly = ly.ravel()[dentro].astype(np.int64)
            px[u] += np.bincount(cel, minlength=len(u)).astype(np.uint16)
            dosel[u] += np.bincount(cel, weights=tc, minlength=len(u)).astype(np.uint32)
            for j, um in enumerate(UMBRALES):
                bos[u, j] += np.bincount(cel[tc >= um], minlength=len(u)).astype(np.uint16)
            p = (ly >= 1) & (ly <= len(ANIOS))
            for j, m in enumerate([p] + [p & (tc >= um) for um in UMBRALES]):
                kk = cel[m] * len(ANIOS) + (ly[m] - 1)
                b = np.bincount(kk, minlength=len(u) * len(ANIOS)).reshape(len(u), len(ANIOS))
                nuevo = perd[u, :, j].astype(np.uint16) + b
                assert nuevo.max() <= 255, 'más de 255 píxeles perdidos en un año en una celda'
                perd[u, :, j] = nuevo.astype(np.uint8)
            log(f'  bloque {k + 1}/{len(bl)} ({bf},{bc}): {dentro.sum():,} píxeles en celdas ({time.time() - t0:.0f} s)')
        listos.add((bf, bc))
        if not args.max_bloques and (len(listos) % 5 == 0 or k == len(bl) - 1):
            np.savez(parcial, px=px, bos=bos, dosel=dosel, perd=perd)
            hechos.write_text('\n'.join(f'{a},{b}' for a, b in sorted(listos)))
    if args.max_bloques:
        log(f'prueba: {int(px.sum()):,} píxeles; celdas con dato {int((px > 0).sum()):,}; '
            f'perdidos {int(perd[:, :, 0].sum(dtype=np.int64)):,}')
        return
    escribir(cen, px, bos, dosel, perd)


def escribir(cen, px, bos, dosel, perd) -> None:
    hay = px > 0
    sin = int((~hay).sum())
    if sin:
        log(f'AVISO: {sin:,} celdas sin píxeles con centro adentro (quedan fuera)')
    a = pd.DataFrame({'h3': cen.h3.values[hay], 'n_px': px[hay].astype(np.int32),
                      'px_dosel10': bos[hay, 0].astype(np.int32), 'px_dosel30': bos[hay, 1].astype(np.int32),
                      'dosel_medio': (dosel[hay] / px[hay]).astype(np.float32)})
    a.to_parquet(HAN / 'hansen_h3.parquet', index=False, compression='zstd')
    ci, ai = np.nonzero(perd[:, :, 0])
    b = pd.DataFrame({'h3': cen.h3.values[ci], 'anio': np.array(ANIOS, np.int16)[ai],
                      'px_perdida': perd[ci, ai, 0].astype(np.int16), 'px_perdida10': perd[ci, ai, 1].astype(np.int16),
                      'px_perdida30': perd[ci, ai, 2].astype(np.int16)})
    b.to_parquet(HAN / 'hansen_h3_anio.parquet', index=False, compression='zstd')
    log(f'parquets: {len(a):,} celdas, {len(b):,} filas celda × año con pérdida')


def cargar() -> None:
    a = pd.read_parquet(HAN / 'hansen_h3.parquet')
    b = pd.read_parquet(HAN / 'hansen_h3_anio.parquet')
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('CREATE SCHEMA IF NOT EXISTS series')
        cur.execute("COMMENT ON SCHEMA series IS 'Hexoma: series anuales por celda (pipeline/hexoma/build_hexoma_*.py)'")
        cur.execute("SELECT relkind FROM pg_class WHERE oid = to_regclass('series.v_hansen_tasas')")
        k = cur.fetchone()
        if k:  # hasta el 6-nov-2026 era una vista común
            cur.execute(f"DROP {'MATERIALIZED VIEW' if k[0] == 'm' else 'VIEW'} series.v_hansen_tasas")
        cur.execute('DROP TABLE IF EXISTS series.hansen_h3_anio, series.hansen_h3 CASCADE')

        def copy(df, tabla, cols):
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)
            cur.copy_expert(f"COPY {tabla} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)
        cur.execute('CREATE TEMP TABLE ta (h3 text, n_px int, px_dosel10 int, px_dosel30 int, dosel_medio real)')
        copy(a, 'ta', 'h3, n_px, px_dosel10, px_dosel30, dosel_medio')
        cur.execute("""CREATE TABLE series.hansen_h3 AS
                       SELECT h3::h3index AS h3, n_px, (h3_cell_area(h3::h3index, 'km^2') * 100)::real AS ha,
                              (h3_cell_area(h3::h3index, 'km^2') * 100 * px_dosel10 / n_px)::real AS bosque10_ha,
                              (h3_cell_area(h3::h3index, 'km^2') * 100 * px_dosel30 / n_px)::real AS bosque30_ha,
                              dosel_medio FROM ta""")
        cur.execute('ALTER TABLE series.hansen_h3 ADD PRIMARY KEY (h3)')
        cur.execute('CREATE TEMP TABLE tb (h3 text, anio smallint, px_perdida smallint, px_perdida10 smallint, px_perdida30 smallint)')
        copy(b, 'tb', 'h3, anio, px_perdida, px_perdida10, px_perdida30')
        cur.execute("""CREATE TABLE series.hansen_h3_anio AS
                       SELECT tb.h3::h3index AS h3, anio, (s.ha * px_perdida / s.n_px)::real AS perdida_ha,
                              (s.ha * px_perdida10 / s.n_px)::real AS perdida10_ha,
                              (s.ha * px_perdida30 / s.n_px)::real AS perdida30_ha, px_perdida
                       FROM tb JOIN series.hansen_h3 s ON s.h3 = tb.h3::h3index""")
        cur.execute('ALTER TABLE series.hansen_h3_anio ADD PRIMARY KEY (h3, anio)')
        cur.execute("""CREATE MATERIALIZED VIEW series.v_hansen_tasas AS
                       SELECT s.h3, s.ha, s.bosque30_ha,
                              100 * coalesce(sum(a.perdida_ha) FILTER (WHERE anio BETWEEN 2001 AND 2010), 0) / s.ha / 10 AS tasa_2001_2010,
                              100 * coalesce(sum(a.perdida_ha) FILTER (WHERE anio BETWEEN 2016 AND 2025), 0) / s.ha / 10 AS tasa_2016_2025,
                              CASE WHEN s.bosque30_ha > 0 THEN 100 * coalesce(sum(a.perdida30_ha), 0) / s.bosque30_ha END AS perdida30_pct_bosque
                       FROM series.hansen_h3 s LEFT JOIN series.hansen_h3_anio a USING (h3) GROUP BY s.h3, s.ha, s.bosque30_ha""")
        cur.execute('CREATE UNIQUE INDEX ON series.v_hansen_tasas (h3)')  # materializada: consultar una celda es inmediato
        F = 'Hansen GFC v1.13 (UMD/hansen/global_forest_change_2025_v1_13), píxel de 1″ (~30 m) con centro en la celda'
        for t, c, d in (
                ('hansen_h3', None, f'Por celda: superficie y bosque 2000. {F}. pipeline/hexoma/build_hexoma_hansen.py'),
                ('hansen_h3', 'n_px', 'píxeles de ~30 m con centro en la celda'),
                ('hansen_h3', 'ha', 'superficie de la celda (h3_cell_area), ha'),
                ('hansen_h3', 'bosque10_ha', 'ha con dosel 2000 ≥ 10 % (umbral FAO): px con dosel ≥ 10 × ha / n_px'),
                ('hansen_h3', 'bosque30_ha', 'ha con dosel 2000 ≥ 30 % (umbral de Global Forest Watch)'),
                ('hansen_h3', 'dosel_medio', '% de dosel medio en 2000 (treecover2000) de los píxeles de la celda'),
                ('hansen_h3_anio', None, f'Pérdida por celda y año, 2001–2025; sólo filas con pérdida (sin fila = 0). {F}. Incluye cosecha de plantaciones: Hansen no separa bosque nativo de plantación'),
                ('hansen_h3_anio', 'perdida_ha', 'ha perdidas en el año, cualquier dosel en 2000'),
                ('hansen_h3_anio', 'perdida10_ha', 'ha perdidas en el año donde el dosel 2000 era ≥ 10 %'),
                ('hansen_h3_anio', 'perdida30_ha', 'ha perdidas en el año donde el dosel 2000 era ≥ 30 % (la cifra de Global Forest Watch)'),
                ('hansen_h3_anio', 'px_perdida', 'píxeles perdidos en el año'),
        ):
            if c:
                cur.execute(f'COMMENT ON COLUMN series.{t}.{c} IS %s', (d,))
            else:
                cur.execute(f'COMMENT ON TABLE series.{t} IS %s', (d,))
        cur.execute("COMMENT ON MATERIALIZED VIEW series.v_hansen_tasas IS %s",
                    ('Tasas por celda: % de la celda perdido por año en 2001–2010 y 2016–2025 (la definición de c_loss_rate '
                     '_baseline y c_loss_rate de la capa web) y % del bosque 2000 (dosel ≥ 30 %) perdido hasta 2025',))
        for c, d in (('tasa_2001_2010', '% de la celda perdido por año, media 2001–2010 (cualquier dosel; = c_loss_rate_baseline)'),
                     ('tasa_2016_2025', '% de la celda perdido por año, media 2016–2025 (cualquier dosel; = c_loss_rate)'),
                     ('perdida30_pct_bosque', '% del bosque 2000 con dosel ≥ 30 % perdido entre 2001 y 2025 (NULL sin bosque)'),
                     ('h3', 'celda H3 res-9'), ('ha', 'superficie de la celda, ha'), ('bosque30_ha', 'ha con dosel 2000 ≥ 30 %')):
            cur.execute(f'COMMENT ON COLUMN series.v_hansen_tasas.{c} IS %s', (d,))
        cur.execute('ANALYZE series.hansen_h3; ANALYZE series.hansen_h3_anio')
        cur.execute("""SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace s
                       ON s.oid = c.relnamespace WHERE s.nspname = 'series' AND c.relkind = 'r'""")
        log(f'esquema series (Hansen): {cur.fetchone()[0]}')


def validar() -> None:
    """Totales por provincia y año contra un reduceRegion de GEE (independiente del reparto en celdas) y tasas por
    celda contra c_loss_rate de la capa web (misma fuente, otro código: zonal por polígono)."""
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute("""SELECT t.territorio, a.anio, sum(a.perdida30_ha), sum(a.perdida_ha)
                   FROM series.hansen_h3_anio a JOIN (SELECT DISTINCT ON (h3) h3, territorio_id FROM hexagonos ORDER BY h3, territorio_id) x USING (h3)
                   JOIN territorios t USING (territorio_id) GROUP BY 1, 2""")
    loc = pd.DataFrame(cur.fetchall(), columns=['territorio', 'anio', 'h3_30', 'h3_todo']).astype({'h3_30': float, 'h3_todo': float})
    # Polígono de cada provincia = unión de las celdas cargadas (lo mismo que suman las celdas), simplificado en GEE.
    img = ee.Image(ASSET)
    area = ee.Image.pixelArea().divide(1e4)
    m30 = img.select('treecover2000').gte(30)
    ly = img.select('lossyear')
    cache = HAN / 'gee_provincias.parquet'
    filas = [] if not cache.exists() else pd.read_parquet(cache).to_dict('records')
    nombres = {'misiones': 'Misiones', 'corrientes': 'Corrientes', 'chaco': 'Chaco', 'formosa': 'Formosa'}
    gaul = ee.FeatureCollection('FAO/GAUL/2015/level1').filter(ee.Filter.eq('ADM0_NAME', 'Argentina'))
    for t, nom in ({} if filas else nombres).items():
        geom = gaul.filter(ee.Filter.eq('ADM1_NAME', nom)).geometry()
        for nombre, mask in (('gee_30', m30), ('gee_todo', None)):
            im = area.updateMask(ly.gt(0) if mask is None else ly.gt(0).And(mask)).addBands(ly)
            r = im.reduceRegion(ee.Reducer.sum().group(1, 'anio'), geom, scale=img.projection().nominalScale(),
                                maxPixels=1e11, tileScale=8).getInfo()
            for gr in r['groups']:
                filas.append({'territorio': t, 'anio': 2000 + int(gr['anio']), 'fuente': nombre, 'ha': gr['sum']})
        log(f'  GEE {t} listo')
    if not cache.exists():
        pd.DataFrame(filas).to_parquet(cache, index=False)  # borrar para recalcular en GEE
    gee = pd.DataFrame(filas).pivot_table(index=['territorio', 'anio'], columns='fuente', values='ha').reset_index()
    v = loc.merge(gee, on=['territorio', 'anio'], how='outer').fillna(0)
    tot = v.groupby('territorio')[['h3_30', 'gee_30', 'h3_todo', 'gee_todo']].sum()
    tot['dif_30_%'] = 100 * (tot.h3_30 / tot.gee_30 - 1)
    tot['dif_todo_%'] = 100 * (tot.h3_todo / tot.gee_todo - 1)
    v['dif_30_%'] = 100 * (v.h3_30 / v.gee_30 - 1)
    anual = v.groupby('territorio')['dif_30_%'].agg(['min', 'max', lambda s: s.abs().median()])
    # contra la capa web (lo que cargó build_hexoma.py en capa_deforestation_dynamics)
    cur.execute("""SELECT corr(w.c_loss_rate, h.tasa_2016_2025), corr(w.c_loss_rate_baseline, h.tasa_2001_2010), count(*),
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(w.c_loss_rate - h.tasa_2016_2025)),
                          percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(w.c_loss_rate - h.tasa_2016_2025))
                   FROM capa_deforestation_dynamics w JOIN series.v_hansen_tasas h ON h.h3 = w.h3
                   WHERE w.principal""")
    r, rb, nw, med, p99 = cur.fetchone()
    with open(VALIDACION, 'w', encoding='utf-8') as f:
        f.write('# Validación de la serie Hansen por celda (`series`)\n\n')
        f.write(f'Generado por `build_hexoma_hansen.py --validar` el {time.strftime("%Y-%m-%d")}.\n\n')
        f.write('## Totales 2001–2025 por provincia contra GEE\n\n')
        f.write('Celdas: suma de `series.hansen_h3_anio` sobre las celdas de cada territorio (píxel → celda de su centro). '
                'GEE: `reduceRegion` de `pixelArea` sobre la provincia de FAO GAUL 2015 nivel 1, a la escala nativa. '
                'Los bordes no son los mismos (GAUL contra los radios INDEC de las celdas) y las celdas de borde entre provincias '
                'cuentan en las dos: la diferencia esperable es de pocos por ciento.\n\n')
        f.write('| Territorio | Celdas, dosel ≥ 30 % (ha) | GEE (ha) | Dif. | Celdas, todo (ha) | GEE (ha) | Dif. | Dif. anual mín./máx./mediana abs. (≥ 30 %) |\n|---|---|---|---|---|---|---|---|\n')
        for t, x in tot.iterrows():
            an = anual.loc[t]
            f.write(f'| {t} | {x.h3_30:,.0f} | {x.gee_30:,.0f} | {x["dif_30_%"]:+.1f} % | {x.h3_todo:,.0f} | {x.gee_todo:,.0f} | '
                    f'{x["dif_todo_%"]:+.1f} % | {an["min"]:+.1f} / {an["max"]:+.1f} / {an.iloc[2]:.1f} % |\n')
        f.write(f'\n## Contra la capa web (`capa_deforestation_dynamics`)\n\n'
                f'Misma fuente, otro código (zonal por polígono de la celda en `process_hansen_to_h3.py`; acá, centro del píxel). '
                f'{nw:,} celdas.\n\n- r de `c_loss_rate` (2016–2025) contra `tasa_2016_2025`: **{r:.4f}**\n'
                f'- r de `c_loss_rate_baseline` (2001–2010) contra `tasa_2001_2010`: **{rb:.4f}**\n'
                f'- diferencia absoluta 2016–2025: mediana {med:.4f}, P99 {p99:.4f} puntos de %/año\n')
        f.write('\n## Por año\n\n| Territorio | Año | Celdas ≥ 30 % (ha) | GEE ≥ 30 % (ha) | Dif. |\n|---|---|---|---|---|\n')
        for _, x in v.sort_values(['territorio', 'anio']).iterrows():
            f.write(f'| {x.territorio} | {int(x.anio)} | {x.h3_30:,.0f} | {x.gee_30:,.0f} | {x["dif_30_%"]:+.1f} % |\n')
    log(f'validación → {VALIDACION.name}')
    print(tot.round(1).to_string())
    print(f'web: r {r:.4f} (actual), {rb:.4f} (base); n {nw:,}; |dif| mediana {med:.4f}, P99 {p99:.4f}')


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--solo-carga', action='store_true')
    ap.add_argument('--validar', action='store_true', help='sólo la validación')
    ap.add_argument('--max-bloques', type=int, help='prueba con N bloques, sin guardar ni cargar')
    args = ap.parse_args()
    if args.validar:
        validar()
        return 0
    if not args.solo_carga:
        procesar(args)
        if args.max_bloques:
            return 0
    cargar()
    validar()
    return 0


if __name__ == '__main__':
    sys.exit(main())
