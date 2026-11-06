"""Hexoma — capas institucionales por celda (esquema `institucional`): OTBN, áreas protegidas y comunidades indígenas.

Fuentes (bajadas a %HEXOMA_FUENTES%/institucional/ con sha256 en institucional.fuentes):
- OTBN (Ley 26.331):
  * Misiones: shapefile del Gobierno de Misiones / IMIBIO, actualización 2017 (Res. 265/2017 del MEyRNR, acreditada
    por la Res. 2/2021 de la SAyDS), copia local en OneDrive/gee/misiones/otbn/otbn_misiones_full.shp. La
    compilación nacional trae para Misiones el OTBN de 2010, ya reemplazado.
  * Corrientes, Chaco y Formosa: compilación nacional de la SAyDS, WFS https://geo2.ambiente.gob.ar/geoserver/ows,
    capa ordenamiento:OTBN_SIG250nov2017 (escala 1:250.000, nov-2017). La SAyDS avisa que hubo post-procesamiento:
    no reproduce exactamente el OTBN aprobado por cada provincia ni las actualizaciones posteriores a 2017.
- Áreas protegidas (mismo WFS): ordenamiento:apn_sifal2016 (nacionales, SIFAP 2016), ordenamiento:app_polig_2016b
  (provinciales, 2016) y areas_protegidas:sitios_ramsar.
- Comunidades indígenas: listado del INAI de comunidades con relevamiento territorial (Ley 26.160) con resolución
  y/o personería jurídica registrada, oct-2025 (datos.mininterior.gob.ar). Punto aproximado cuando lo trae.

Asignación a la celda H3 res-9: polígonos → celdas que tocan y %
de la celda = área de la intersección (geography) / área de la celda. Puntos → celda que los contiene. Sólo celdas
de `hexagonos` (NEA argentino). Polígonos grandes partidos con ST_Subdivide; join espacial contra el polígono de cada
celda (índice GiST). h3-pg 4.1 no trae el modo 'overlapping' de polygon_to_cells: por centro se perdían los
polígonos chicos del OTBN, que vienen de rásters (muchos de menos de 1 ha).

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_institucional.py              # baja (si falta), carga, cruza y valida
    python pipeline/hexoma/build_hexoma_institucional.py --rebajar    # vuelve a bajar las fuentes
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
INS = FUENTES_DIR / 'institucional'
WFS = 'https://geo2.ambiente.gob.ar/geoserver/ows'
OTBN_MISIONES = Path(r'C:\Users\ant\OneDrive\gee\misiones\otbn\otbn_misiones_full.shp')
INAI = ('http://datos.mininterior.gob.ar/dataset/6a702e6f-3292-44fe-804f-16b8dee008c1/resource/'
        '3c2137d5-46d3-40cf-9e01-ee406746b27a/download/listado-de-comunidades-indigenas-20251112.csv')
PROV = {'54': 'Misiones', '18': 'Corrientes', '22': 'Chaco', '34': 'Formosa'}
OTBN_WFS_PROV = ['Corrientes', 'Chaco', 'Formosa']
CAPAS_AP = {'nacional': 'ordenamiento:apn_sifal2016', 'provincial': 'ordenamiento:app_polig_2016b',
            'ramsar': 'areas_protegidas:sitios_ramsar'}
# Superficie acreditada del OTBN de Misiones (Res. RESOL-2021-2-APN-SPARN#MAD), ha
OTBN_MISIONES_OFICIAL = {'I': 233083, 'II': 901617, 'III': 477858}
VALIDACION = Path(__file__).with_name('validacion_institucional.md')
UA = {'User-Agent': 'Mozilla/5.0 (hexoma; investigacion)'}


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def wfs_todo(capa: str, destino: Path, cql: str | None = None, orden: str | None = None, pagina: int = 5000) -> int:
    """GetFeature WFS 2.0 paginado → un GeoJSON. Devuelve la cantidad de objetos."""
    feats, start = [], 0
    while True:
        p = {'SERVICE': 'WFS', 'VERSION': '2.0.0', 'REQUEST': 'GetFeature', 'typeNames': capa,
             'outputFormat': 'application/json', 'count': pagina, 'startIndex': start, 'srsName': 'EPSG:4326'}
        if cql:
            p['CQL_FILTER'] = cql
        if orden:
            p['sortBy'] = orden
        for intento in range(5):
            try:
                r = requests.get(WFS, params=p, headers=UA, timeout=600)
                r.raise_for_status()
                d = r.json()
                break
            except Exception:
                if intento == 4:
                    raise
                time.sleep(15 * (intento + 1))
        feats += d['features']
        log(f'  {capa} {cql or ""}: {len(feats):,}')
        if len(d['features']) < pagina:
            break
        start += pagina
    destino.write_text(json.dumps({'type': 'FeatureCollection', 'features': feats}), encoding='utf-8')
    return len(feats)


def bajar(rebajar: bool) -> list[dict]:
    INS.mkdir(parents=True, exist_ok=True)
    fuentes = []

    def reg(nombre, url, archivo: Path, n, nota=''):
        fuentes.append({'nombre': nombre, 'url': url, 'archivo': archivo.name, 'bytes': archivo.stat().st_size,
                        'sha256': sha256(archivo), 'objetos': n, 'bajado': time.strftime('%Y-%m-%d'), 'nota': nota})

    for prov in OTBN_WFS_PROV:
        f = INS / f'otbn_{prov.lower()}.geojson'
        if rebajar or not f.exists():
            wfs_todo('ordenamiento:OTBN_SIG250nov2017', f, cql=f"provincia ILIKE '%{prov}%'", orden='id_')
        reg(f'OTBN {prov} (compilación nacional SAyDS, nov-2017)', f'{WFS} ordenamiento:OTBN_SIG250nov2017', f,
            len(json.loads(f.read_text(encoding='utf-8'))['features']))
    dm = INS / 'otbn_misiones_imibio'
    if rebajar or not (dm / OTBN_MISIONES.name).exists():
        dm.mkdir(exist_ok=True)
        for ext in ('.shp', '.shx', '.dbf', '.prj'):
            shutil.copy2(OTBN_MISIONES.with_suffix(ext), dm / OTBN_MISIONES.with_suffix(ext).name)
    reg('OTBN Misiones (IMIBIO, actualización 2017, acreditada Res. 2/2021)', str(OTBN_MISIONES), dm / OTBN_MISIONES.name,
        109378, 'copia local; ver OneDrive/gee/misiones/otbn/README_otbn.md')
    for tipo, capa in CAPAS_AP.items():
        f = INS / f'ap_{tipo}.geojson'
        if rebajar or not f.exists():
            wfs_todo(capa, f)
        reg(f'Áreas protegidas {tipo} ({capa})', f'{WFS} {capa}', f, len(json.loads(f.read_text(encoding='utf-8'))['features']))
    f = INS / 'inai_comunidades_202510.csv'
    if rebajar or not f.exists():
        r = requests.get(INAI, headers=UA, timeout=300)
        r.raise_for_status()
        f.write_bytes(r.content)
    reg('INAI: comunidades con relevamiento y/o personería, oct-2025', INAI, f, None)
    (INS / 'fuentes.json').write_text(json.dumps(fuentes, ensure_ascii=False, indent=1), encoding='utf-8')
    return fuentes


def arreglar_texto(s):
    """Algunos atributos del WFS vienen con UTF-8 leído como Latin-1 («PiÃ±alito»)."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def leer_inai() -> pd.DataFrame:
    raw = (INS / 'inai_comunidades_202510.csv').read_bytes()
    for enc in ('utf-8-sig', 'latin-1'):
        try:
            txt = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    d = pd.read_csv(io.StringIO(txt), sep=';', dtype=str)
    d.columns = [c.strip().replace(' ', '') for c in d.columns]
    d = d[d.provincia_id.str.strip().str.zfill(2).isin(PROV)].copy()
    for c in ('comunidad_latitud', 'comunidad_longitud'):
        d[c] = pd.to_numeric(d[c].str.strip().str.replace(',', '.'), errors='coerce')
    d = d.apply(lambda s: s.str.strip() if s.dtype == object else s)
    return d


def cargar(fuentes: list[dict]) -> None:
    import geopandas as gpd
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('SET client_min_messages = warning')
    cur.execute('DROP SCHEMA IF EXISTS institucional CASCADE')
    cur.execute('CREATE SCHEMA institucional')
    cur.execute("COMMENT ON SCHEMA institucional IS 'Hexoma: OTBN, áreas protegidas y comunidades indígenas por celda. "
                "pipeline/hexoma/build_hexoma_institucional.py'")
    cur.execute("""CREATE TABLE institucional.fuentes (nombre text, url text, archivo text, bytes bigint, sha256 text,
                   objetos int, bajado date, nota text)""")
    for f in fuentes:
        cur.execute('INSERT INTO institucional.fuentes VALUES (%(nombre)s, %(url)s, %(archivo)s, %(bytes)s, %(sha256)s, '
                    '%(objetos)s, %(bajado)s, %(nota)s)', f)

    def subir_wkb(gdf, tabla, cols):
        buf = io.StringIO()
        out = gdf[cols].copy()
        out['geom'] = gdf.geometry.apply(lambda g: g.wkb_hex if g is not None else None)
        out.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert(f"COPY {tabla} ({', '.join(cols)}, geom_wkb) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)

    # OTBN
    cur.execute('CREATE TEMP TABLE o (provincia text, cat_cons text, periodo text, fuente text, geom_wkb text)')
    for prov in OTBN_WFS_PROV:
        g = gpd.read_file(INS / f'otbn_{prov.lower()}.geojson')
        g['provincia'] = prov
        g['periodo'] = g['periodo'].astype(str)
        g['fuente'] = 'SAyDS OTBN_SIG250nov2017'
        subir_wkb(g, 'o', ['provincia', 'cat_cons', 'periodo', 'fuente'])
    g = gpd.read_file(INS / 'otbn_misiones_imibio' / OTBN_MISIONES.name).to_crs(4326)
    g['provincia'], g['periodo'], g['fuente'] = 'Misiones', '2017', 'IMIBIO actualización 2017'
    subir_wkb(g, 'o', ['provincia', 'cat_cons', 'periodo', 'fuente'])
    cur.execute("""CREATE TABLE institucional.otbn AS
                   SELECT row_number() OVER () AS id, provincia, upper(trim(cat_cons)) AS cat_cons, periodo, fuente,
                          ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_GeomFromWKB(decode(geom_wkb, 'hex'), 4326)), 3))::geometry(MultiPolygon, 4326) AS geom
                   FROM o WHERE geom_wkb IS NOT NULL""")
    cur.execute("DELETE FROM institucional.otbn WHERE ST_IsEmpty(geom) OR cat_cons NOT IN ('I', 'II', 'III')")
    cur.execute('CREATE INDEX ON institucional.otbn USING gist (geom)')
    cur.execute('SELECT provincia, cat_cons, periodo, count(*) FROM institucional.otbn GROUP BY 1, 2, 3 ORDER BY 1, 2, 3')
    log('otbn: ' + '; '.join(f'{p} {c} ({pe}) {n:,}' for p, c, pe, n in cur.fetchall()))
    conn.commit()

    # Áreas protegidas
    cur.execute("""CREATE TEMP TABLE a (tipo text, nombre text, categoria text, uicn text, provincia text, creacion text,
                   instrumento text, sup_declarada_ha real, geom_wkb text)""")
    for tipo in CAPAS_AP:
        g = gpd.read_file(INS / f'ap_{tipo}.geojson')
        if g.crs is None:
            g = g.set_crs(4326)
        g = g.to_crs(4326)
        pick = lambda *cs: next((g[c] for c in cs if c in g.columns), pd.Series([None] * len(g)))  # noqa: E731
        df = gpd.GeoDataFrame({
            'tipo': tipo, 'nombre': pick('nombre').map(arreglar_texto), 'categoria': pick('categoria').map(arreglar_texto),
            'uicn': pick('uicn_cat', 'categ_uicn'), 'provincia': pick('provincia').map(arreglar_texto),
            'creacion': pick('creacion', 'fecha').astype(str), 'instrumento': pick('instrument').map(arreglar_texto),
            'sup_declarada_ha': pd.to_numeric(pick('sup_total', 'superficie'), errors='coerce')}, geometry=g.geometry)
        subir_wkb(df, 'a', ['tipo', 'nombre', 'categoria', 'uicn', 'provincia', 'creacion', 'instrumento', 'sup_declarada_ha'])
    cur.execute("""CREATE TABLE institucional.areas_protegidas AS
                   SELECT row_number() OVER () AS ap_id, tipo, nombre, categoria, uicn, provincia, creacion, instrumento,
                          sup_declarada_ha,
                          ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_GeomFromWKB(decode(geom_wkb, 'hex'), 4326)), 3))::geometry(MultiPolygon, 4326) AS geom
                   FROM a WHERE geom_wkb IS NOT NULL""")
    # sólo las que tocan el NEA argentino cargado
    cur.execute("""DELETE FROM institucional.areas_protegidas p WHERE NOT EXISTS (
                     SELECT 1 FROM limites l WHERE ST_Intersects(l.geom, p.geom))""")
    cur.execute('CREATE INDEX ON institucional.areas_protegidas USING gist (geom)')
    conn.commit()

    # Comunidades INAI
    d = leer_inai()
    cols = ['comunidad_id', 'comunidad_nombre', 'comunidad_pueblo', 'provincia_id', 'departamento_nombre',
            'localidad_nombre', 'comunidad_zona_localizacion', 'comunidad_latitud', 'comunidad_longitud',
            'personeria_juridica_estado', 'personeria_tipo_inscripcion', 'relevamiento_estado', 'relevamiento_fecha',
            'relevamiento_resolucion_inai']
    cols = [c for c in cols if c in d.columns]
    cur.execute('CREATE TABLE institucional.comunidades (' + ', '.join(
        f'{c} {"double precision" if c in ("comunidad_latitud", "comunidad_longitud") else "text"}' for c in cols) + ')')
    buf = io.StringIO()
    d[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert(f"COPY institucional.comunidades ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""ALTER TABLE institucional.comunidades ADD COLUMN geom geometry(Point, 4326), ADD COLUMN h3 h3index,
                   ADD COLUMN territorio_ok boolean""")
    cur.execute("""UPDATE institucional.comunidades SET geom = ST_SetSRID(ST_MakePoint(comunidad_longitud, comunidad_latitud), 4326),
                          h3 = h3_lat_lng_to_cell(point(comunidad_longitud, comunidad_latitud), 9)
                   WHERE comunidad_latitud BETWEEN -35 AND -20 AND comunidad_longitud BETWEEN -66 AND -52""")
    cur.execute("""UPDATE institucional.comunidades c SET territorio_ok = EXISTS (
                     SELECT 1 FROM hexagonos x JOIN territorios t USING (territorio_id)
                     WHERE x.h3 = c.h3 AND lower(t.territorio) = lower(translate(%s::jsonb ->> lpad(c.provincia_id, 2, '0'), 'ÁÉÍÓÚ', 'AEIOU')))
                   WHERE h3 IS NOT NULL""", (json.dumps(PROV),))
    conn.commit()
    log('cargadas fuentes, otbn, áreas protegidas y comunidades')
    cruzar(conn)
    comentar(conn)


def cruzar(conn) -> None:
    cur = conn.cursor()
    t0 = time.time()
    # Celdas del NEA cargadas (una fila por h3)
    cur.execute('CREATE TEMP TABLE celdas AS SELECT h3, h3_cell_to_boundary_geometry(h3) AS geom FROM (SELECT DISTINCT h3 FROM hexagonos) x')
    cur.execute('CREATE INDEX ON celdas USING gist (geom)')
    cur.execute('ANALYZE celdas')
    log(f'  celdas con polígono e índice: {time.time() - t0:.0f} s')
    # OTBN: polígonos de la fuente (partidos) → celdas que tocan → área de la intersección
    cur.execute("""CREATE TEMP TABLE op AS SELECT cat_cons, fuente, ST_Subdivide(geom, 256) AS geom FROM institucional.otbn""")
    cur.execute('CREATE INDEX ON op USING gist (geom)')
    cur.execute("""CREATE TEMP TABLE oc AS
                   SELECT o.cat_cons, o.fuente, c.h3,
                          CASE WHEN ST_Within(c.geom, o.geom) THEN ST_Area(c.geom::geography)
                               ELSE ST_Area(ST_Intersection(o.geom, c.geom)::geography) END AS m2
                   FROM op o JOIN celdas c ON ST_Intersects(o.geom, c.geom)""")
    log(f'  otbn × celdas: {time.time() - t0:.0f} s')
    cur.execute("""CREATE TABLE institucional.otbn_h3 AS
                   SELECT h3, least(100, 100 * coalesce(sum(m2) FILTER (WHERE cat_cons = 'I'), 0) / h3_cell_area(h3, 'm^2'))::real AS cat_i_pct,
                          least(100, 100 * coalesce(sum(m2) FILTER (WHERE cat_cons = 'II'), 0) / h3_cell_area(h3, 'm^2'))::real AS cat_ii_pct,
                          least(100, 100 * coalesce(sum(m2) FILTER (WHERE cat_cons = 'III'), 0) / h3_cell_area(h3, 'm^2'))::real AS cat_iii_pct,
                          min(fuente) AS fuente
                   FROM oc WHERE m2 > 0 GROUP BY h3""")
    # OJO: least() de Postgres ignora los NULL (least(100, NULL) = 100): el coalesce va adentro, no después.
    cur.execute('ALTER TABLE institucional.otbn_h3 ADD PRIMARY KEY (h3)')
    # Áreas protegidas
    cur.execute('CREATE TEMP TABLE app AS SELECT ap_id, ST_Subdivide(geom, 256) AS geom FROM institucional.areas_protegidas')
    cur.execute('CREATE INDEX ON app USING gist (geom)')
    cur.execute("""CREATE TABLE institucional.ap_h3 AS
                   SELECT c.h3, p.ap_id,
                          least(100, 100 * sum(CASE WHEN ST_Within(c.geom, p.geom) THEN ST_Area(c.geom::geography)
                                                    ELSE ST_Area(ST_Intersection(p.geom, c.geom)::geography) END)
                                / h3_cell_area(c.h3, 'm^2'))::real AS pct
                   FROM app p JOIN celdas c ON ST_Intersects(p.geom, c.geom) GROUP BY c.h3, p.ap_id""")
    cur.execute('DELETE FROM institucional.ap_h3 WHERE pct <= 0')
    cur.execute('ALTER TABLE institucional.ap_h3 ADD PRIMARY KEY (h3, ap_id)')
    # Vista por celda
    cur.execute("""CREATE VIEW institucional.v_h3 AS
                   SELECT c.h3, o.cat_i_pct, o.cat_ii_pct, o.cat_iii_pct,
                          CASE WHEN o.h3 IS NULL THEN NULL
                               WHEN o.cat_i_pct >= greatest(o.cat_ii_pct, o.cat_iii_pct) THEN 'I'
                               WHEN o.cat_ii_pct >= o.cat_iii_pct THEN 'II' ELSE 'III' END AS otbn_cat_mayor,
                          a.ap_pct, a.ap_nombres, a.ap_tipos, coalesce(k.n, 0) AS n_comunidades
                   FROM (SELECT DISTINCT h3 FROM hexagonos) c
                   LEFT JOIN institucional.otbn_h3 o USING (h3)
                   LEFT JOIN (SELECT h3, least(100, sum(pct))::real AS ap_pct,
                                     string_agg(DISTINCT p.nombre, ' · ') AS ap_nombres, string_agg(DISTINCT p.tipo, ',') AS ap_tipos
                              FROM institucional.ap_h3 JOIN institucional.areas_protegidas p USING (ap_id) GROUP BY h3) a USING (h3)
                   LEFT JOIN (SELECT h3, count(*) AS n FROM institucional.comunidades WHERE h3 IS NOT NULL GROUP BY h3) k USING (h3)""")
    cur.execute('ANALYZE institucional.otbn_h3; ANALYZE institucional.ap_h3')
    conn.commit()
    log(f'cruce institucional: {time.time() - t0:.0f} s')


def comentar(conn) -> None:
    cur = conn.cursor()
    C = {
        ('otbn', None): 'Polígonos del OTBN (Ley 26.331): Misiones = IMIBIO actualización 2017 (acreditada Res. 2/2021); '
                        'Corrientes, Chaco y Formosa = compilación nacional SAyDS OTBN_SIG250nov2017 (1:250.000; con '
                        'post-procesamiento de la SAyDS, no incluye actualizaciones provinciales posteriores). cat_cons: I rojo '
                        '(no transformar), II amarillo (aprovechamiento sostenible), III verde (transformación parcial posible)',
        ('otbn_h3', None): '% de la superficie de cada celda en cada categoría del OTBN (intersección exacta con el polígono '
                           'de la celda, área geodésica). Celda ausente = sin bosque nativo zonificado',
        ('otbn_h3', 'fuente'): 'versión del OTBN usada en la celda',
        ('areas_protegidas', None): 'Áreas protegidas que tocan el NEA argentino: nacionales (SIFAP 2016, apn_sifal2016), '
                                    'provinciales (app_polig_2016b, 2016) y sitios Ramsar (WFS SAyDS). sup_declarada_ha = '
                                    'superficie del atributo de la fuente',
        ('ap_h3', None): 'Celda × área protegida: % de la celda dentro del área (las áreas pueden superponerse: '
                         'un sitio Ramsar contiene parques)',
        ('comunidades', None): 'Comunidades indígenas de Misiones, Corrientes, Chaco y Formosa del listado del INAI '
                               '(relevamiento Ley 26.160 con resolución y/o personería registrada, oct-2025). geom/h3 sólo '
                               'cuando la fuente trae coordenadas; son aproximadas. territorio_ok = el punto cae en una '
                               'celda de su provincia',
        ('v_h3', None): 'Por celda: OTBN, áreas protegidas (ap_pct tope 100 si se superponen) y comunidades con punto en la celda',
    }
    pct = '% de la superficie de la celda en la categoría {} del OTBN ({}); 0 = nada'
    for t in ('otbn_h3', 'v_h3'):
        for c, cat, nom in (('cat_i_pct', 'I', 'rojo: no transformar'), ('cat_ii_pct', 'II', 'amarillo: aprovechamiento sostenible'),
                            ('cat_iii_pct', 'III', 'verde: transformación parcial posible')):
            C[(t, c)] = pct.format(cat, nom)
    C.update({
        ('otbn', 'id'): 'identificador de la carga (no es el de la fuente)', ('otbn', 'cat_cons'): 'categoría de conservación: I, II o III',
        ('fuentes', 'url'): 'origen (URL del servicio o ruta de la copia local)', ('fuentes', 'archivo'): 'archivo en %HEXOMA_FUENTES%/institucional',
        ('fuentes', 'bytes'): 'tamaño del archivo', ('fuentes', 'sha256'): 'sha256 del archivo', ('fuentes', 'objetos'): 'objetos (polígonos) en el archivo',
        ('fuentes', 'bajado'): 'fecha de la bajada',
        ('v_h3', 'otbn_cat_mayor'): 'categoría con más superficie en la celda (NULL = sin OTBN)',
        ('v_h3', 'ap_pct'): '% de la celda en áreas protegidas (suma con tope 100: se superponen)',
        ('v_h3', 'ap_nombres'): 'áreas protegidas que tocan la celda', ('v_h3', 'ap_tipos'): 'nacional, provincial o ramsar',
        ('v_h3', 'n_comunidades'): 'comunidades del INAI con punto en la celda',
        ('ap_h3', 'ap_id'): 'área (institucional.areas_protegidas)', ('ap_h3', 'pct'): '% de la celda dentro del área',
        ('areas_protegidas', 'ap_id'): 'identificador de la carga', ('areas_protegidas', 'uicn'): 'categoría UICN según la fuente',
        ('areas_protegidas', 'creacion'): 'año (o fecha) de creación según la fuente', ('areas_protegidas', 'instrumento'): 'instrumento legal de creación',
        ('areas_protegidas', 'sup_declarada_ha'): 'superficie del atributo de la fuente, ha (puede diferir del polígono)',
    })
    for c, d in (('comunidad_id', 'código del INAI'), ('comunidad_nombre', 'nombre'), ('comunidad_pueblo', 'pueblo'),
                 ('provincia_id', 'código INDEC de provincia'), ('departamento_nombre', 'departamento'), ('localidad_nombre', 'localidad'),
                 ('comunidad_zona_localizacion', 'zona (urbana, periurbana, rural…)'),
                 ('comunidad_latitud', 'latitud aproximada (NULL si no la trae)'), ('comunidad_longitud', 'longitud aproximada'),
                 ('personeria_juridica_estado', 'estado de la personería jurídica'), ('personeria_tipo_inscripcion', 'registro: nacional o provincial'),
                 ('relevamiento_estado', 'estado del relevamiento territorial (Ley 26.160)'), ('relevamiento_fecha', 'fecha del relevamiento'),
                 ('relevamiento_resolucion_inai', 'resolución del INAI que aprueba el relevamiento'),
                 ('territorio_ok', 'el punto cae en una celda de su provincia'), ('geom', 'punto aproximado'), ('h3', 'celda res-9 del punto')):
        C[('comunidades', c)] = d + '. INAI, oct-2025'
    for (t, c), d in C.items():
        kind = 'VIEW' if t.startswith('v_') else 'TABLE'
        cur.execute(f'COMMENT ON {kind} institucional.{t} IS %s' if c is None else f'COMMENT ON COLUMN institucional.{t}.{c} IS %s', (d,))
    conn.commit()


def validar() -> None:
    conn = connect(DB)
    cur = conn.cursor()
    L = ['# Validación de las capas institucionales (`institucional`)\n',
         f'Generado por `build_hexoma_institucional.py` el {time.strftime("%Y-%m-%d")}.\n']
    # OTBN: polígonos contra celdas, por provincia y categoría; Misiones contra lo acreditado
    cur.execute("""WITH pol AS (SELECT provincia, cat_cons, sum(ST_Area(geom::geography)) / 1e4 AS ha FROM institucional.otbn GROUP BY 1, 2),
                   cel AS (SELECT t.territorio, sum(o.cat_i_pct * h3_cell_area(o.h3, 'm^2')) / 1e6 AS i,
                                  sum(o.cat_ii_pct * h3_cell_area(o.h3, 'm^2')) / 1e6 AS ii,
                                  sum(o.cat_iii_pct * h3_cell_area(o.h3, 'm^2')) / 1e6 AS iii
                           FROM institucional.otbn_h3 o
                           JOIN (SELECT DISTINCT ON (h3) h3, territorio_id FROM hexagonos ORDER BY h3, territorio_id) x USING (h3)
                           JOIN territorios t USING (territorio_id) GROUP BY 1)
                   SELECT p.provincia, p.cat_cons, p.ha,
                          CASE p.cat_cons WHEN 'I' THEN c.i WHEN 'II' THEN c.ii ELSE c.iii END AS ha_celdas
                   FROM pol p LEFT JOIN cel c ON lower(c.territorio) = lower(p.provincia) ORDER BY 1, 2""")
    L.append('\n## OTBN: superficie de los polígonos contra la suma en celdas\n\n'
             'Celdas en dos provincias cuentan en las dos (948 en todo el NEA): la suma en celdas puede pasar apenas a la del polígono.\n\n'
             '| Provincia | Cat. | Polígonos (ha) | Celdas (ha) | Dif. | Acreditado (ha) | Polígonos / acreditado |\n|---|---|---|---|---|---|---|\n')
    for prov, cat, ha, hc in cur.fetchall():
        of = OTBN_MISIONES_OFICIAL.get(cat) if prov == 'Misiones' else None
        L.append(f'| {prov} | {cat} | {ha:,.0f} | {(hc or 0):,.0f} | {100 * ((hc or 0) / ha - 1):+.1f} % | '
                 f'{f"{of:,}" if of else "—"} | {f"{100 * (ha / of - 1):+.1f} %" if of else "—"} |\n')
    # Áreas protegidas
    cur.execute("""SELECT p.tipo, p.nombre, p.sup_declarada_ha, ST_Area(p.geom::geography) / 1e4,
                          (SELECT sum(a.pct * h3_cell_area(a.h3, 'm^2')) / 1e6 FROM institucional.ap_h3 a WHERE a.ap_id = p.ap_id)
                   FROM institucional.areas_protegidas p ORDER BY 1, 4 DESC""")
    L.append('\n## Áreas protegidas: declarada, polígono y celdas\n\nLas celdas cubren sólo el NEA argentino cargado; un área que '
             'cruza a otra provincia o país queda recortada.\n\n| Tipo | Nombre | Declarada (ha) | Polígono (ha) | En celdas (ha) |\n|---|---|---|---|---|\n')
    for tipo, nom, dec, pol, cel in cur.fetchall():
        L.append(f'| {tipo} | {nom} | {f"{dec:,.0f}" if dec else "—"} | {pol:,.0f} | {(cel or 0):,.0f} |\n')
    # Comunidades
    cur.execute("""SELECT provincia_id, count(*), count(geom), count(*) FILTER (WHERE territorio_ok),
                          count(*) FILTER (WHERE relevamiento_estado ILIKE 'culminado%')
                   FROM institucional.comunidades GROUP BY 1 ORDER BY 1""")
    L.append('\n## Comunidades INAI\n\n| Provincia | Comunidades | Con coordenadas | En una celda de su provincia | Relevamiento culminado |\n|---|---|---|---|---|\n')
    for pid, n, ng, ok, rel in cur.fetchall():
        L.append(f'| {PROV.get(pid.zfill(2), pid)} | {n} | {ng} | {ok} | {rel} |\n')
    VALIDACION.write_text(''.join(L), encoding='utf-8')
    log(f'validación → {VALIDACION.name}')
    print(''.join(L))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--rebajar', action='store_true')
    ap.add_argument('--validar', action='store_true', help='sólo la validación')
    args = ap.parse_args()
    if not args.validar:
        cargar(bajar(args.rebajar))
    validar()
    return 0


if __name__ == '__main__':
    sys.exit(main())
