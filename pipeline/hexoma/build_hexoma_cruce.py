"""Hexoma — cruce único radio → celda H3 res-9 para los censos 1991, 2001, 2010 y 2022 (esquema `cruce`).

Reparte cada radio censal entre sus celdas con un peso de época, elegido por validación.

Pesos por celda (cruce.celda_pesos):
    edificios     edificios de Open Buildings (~2023) en la celda, de los cruces 2022 de la web
                  (pipeline/output/**/h3_radio_crosswalk*.parquet, n_buildings sumado por celda)
    ghsl_<época>  m² construidos según GHS-BUILT-S R2023A en 1990, 2000, 2010 y 2020
                  (gee_export_ghsl_epochs.py; píxel de 100 m asignado a la celda de su centro)

Fracciones (cruce.celda_radio): para cada año, la parte de cada celda que cae en cada radio
(1 si la celda está entera adentro). Sólo se calculan para las celdas con algún peso (edificios
o construcción en alguna época), más las celdas de los radios que no tienen ninguna.

Métodos (peso de una celda dentro de un radio = fracción × …):
    A  área            superficie (sin peso: densidad pareja en el radio)
    B  edificios       edificios de ~2023
    C  ghsl            m² construidos de la época del censo
    D  edificios_ghsl  edificios de ~2023 sólo donde GHSL ya marcaba construcción en la época,
                       en la celda o en su anillo 1
Si un radio queda sin peso con su método, cae al siguiente: D → B → C → A. Cada fila del cruce
dice el método que se usó de verdad.

Validación (cruce.validacion, hexoma/validacion_cruce.md): para cada año y método, se reparte
la población de cada fracción censal entre sus radios según la suma de pesos de cada radio, y se
compara con la población real del radio. Error absoluto ponderado (WAPE = Σ|est − real| / Σ real)
y error mediano por radio, por tamaño del radio. Se excluyen las fracciones de un solo radio.
Regla fijada antes de mirar los resultados: gana el WAPE total más bajo; si otro queda a menos de
1 punto, gana el más simple (A < B < C < D). 2022 conserva el cruce por edificio asignado por punto
(censo_2022.cruce, el de las capas de la web) salvo que gane A o C.

Cruce final (cruce.radio_h3): (anio, h3, territorio_id, redcode, peso, metodo); peso suma 1 por
radio. Confianza por celda y año (cruce.celda_confianza): radios que aportan, personas estimadas
y la parte de esas personas que viene de radios de más de 10 km².

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_cruce.py               # todo
    python pipeline/hexoma/build_hexoma_cruce.py --desde validacion
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import h3
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'pipeline' / 'output'
GHSL_DIR = OUTPUT / 'hexoma' / 'ghsl'
GCS = 'gs://spatia-satellite/satellite/nea_ar'
TMP = OUTPUT / 'hexoma' / 'tmp'
EPOCA = {1991: 1990, 2001: 2000, 2010: 2010, 2022: 2020}
METODOS = ['area', 'edificios', 'ghsl', 'edificios_ghsl']  # de más simple a más complejo
CROSSWALKS = [OUTPUT / 'h3_radio_crosswalk.parquet',
              OUTPUT / 'corrientes' / 'h3_radio_crosswalk_corrientes.parquet',
              OUTPUT / 'chaco' / 'h3_radio_crosswalk_chaco.parquet',
              OUTPUT / 'formosa' / 'h3_radio_crosswalk_formosa.parquet']
RURAL_KM2 = 10


def sh(cur, sql: str, msg: str | None = None):
    t0 = time.time()
    cur.execute(sql)
    if msg:
        log(f'  {msg}: {time.time() - t0:.0f} s' + (f', {cur.rowcount:,} filas' if cur.rowcount >= 0 else ''))


# ── 1. pesos por celda ────────────────────────────────────────────────────────

def ghsl_por_celda(epoca: int) -> pd.DataFrame:
    import rasterio
    from rasterio.windows import Window
    f = GHSL_DIR / f'ghsl_built_{epoca}.tif'
    if not f.exists():
        GHSL_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run(['gcloud', 'storage', 'cp', f'{GCS}/ghsl_built_{epoca}.tif', str(f)], check=True, shell=True)
    partes = []
    with rasterio.open(f) as src:
        T = src.transform
        for r0 in range(0, src.height, 2048):
            w = Window(0, r0, src.width, min(2048, src.height - r0))
            a = src.read(1, window=w)
            rr, cc = np.nonzero(a)
            if not len(rr):
                continue
            lng = T.c + (cc + 0.5) * T.a
            lat = T.f + (r0 + rr + 0.5) * T.e
            cel = [h3.latlng_to_cell(y, x, 9) for y, x in zip(lat, lng)]
            partes.append(pd.DataFrame({'h3': cel, 'm2': a[rr, cc].astype('float64')}).groupby('h3').m2.sum())
    s = pd.concat(partes).groupby(level=0).sum()
    log(f'  GHSL {epoca}: {len(s):,} celdas con construcción, {s.sum() / 1e6:,.1f} km² construidos')
    return s.rename(f'ghsl_{epoca}')


def pesos(conn) -> None:
    edif = duckdb.sql(' UNION ALL '.join(
        f"SELECT h3index AS h3, n_buildings FROM '{p.as_posix()}'" for p in CROSSWALKS)
    ).df().groupby('h3').n_buildings.sum().rename('edificios')
    log(f'  edificios: {len(edif):,} celdas, {int(edif.sum()):,} edificios')
    df = pd.concat([edif] + [ghsl_por_celda(e) for e in sorted(set(EPOCA.values()))], axis=1).fillna(0)
    df.index.name = 'h3'
    # el rectángulo de GHSL toca Paraguay, Brasil y otras provincias: quedan las celdas del Hexoma
    hexa = set(pd.read_sql('SELECT DISTINCT h3::text AS h3 FROM hexagonos', conn).h3)
    df = df[df.index.isin(hexa) | (df.edificios > 0)]
    log(f'  celdas con algún peso dentro del NEA argentino: {len(df):,}')
    TMP.mkdir(parents=True, exist_ok=True)
    f = TMP / 'celda_pesos.csv'
    df.reset_index().to_csv(f, index=False, header=False)
    ep = sorted(set(EPOCA.values()))
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS cruce CASCADE')
        cur.execute('CREATE SCHEMA cruce')
        cur.execute("COMMENT ON SCHEMA cruce IS 'Cruce único radio → celda H3 res-9 de los censos 1991-2022, con pesos de época elegidos por validación (pipeline/hexoma/build_hexoma_cruce.py)'")
        cur.execute(f"""CREATE TABLE cruce.celda_pesos (h3 h3index PRIMARY KEY, edificios real NOT NULL,
                        {', '.join(f'ghsl_{e} real NOT NULL' for e in ep)})""")
        with open(f, encoding='utf-8') as fh:
            cur.copy_expert(f"COPY cruce.celda_pesos FROM STDIN WITH (FORMAT csv)", fh)
        log(f'  celda_pesos: {cur.rowcount:,} celdas')
        cur.execute("COMMENT ON TABLE cruce.celda_pesos IS 'Peso de cada celda: edificios de Open Buildings (~2023, de los cruces 2022 de la web) y m² construidos según GHS-BUILT-S R2023A (píxel de 100 m en EPSG:4326 asignado a la celda de su centro) en 1990, 2000, 2010 y 2020'")
        # máscara de época: construcción GHSL en la celda o en su anillo 1
        for e in ep:
            cur.execute(f"""ALTER TABLE cruce.celda_pesos ADD COLUMN construida_{e} boolean""")
            sh(cur, f"""UPDATE cruce.celda_pesos p SET construida_{e} = EXISTS (
                            SELECT 1 FROM cruce.celda_pesos q WHERE q.h3 = ANY (ARRAY(SELECT h3_grid_disk(p.h3, 1)))
                            AND q.ghsl_{e} > 0)""", f'máscara {e}')
        cur.execute("""CREATE TABLE cruce.celda_geom AS
                       SELECT h3, h3_cell_to_boundary_geometry(h3) AS geom FROM cruce.celda_pesos""")
        cur.execute('CREATE INDEX ON cruce.celda_geom USING gist (geom); ALTER TABLE cruce.celda_geom ADD PRIMARY KEY (h3)')
    f.unlink()


# ── 2. fracciones celda ∩ radio ───────────────────────────────────────────────

def fracciones(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP TABLE IF EXISTS cruce.celda_radio')
        cur.execute("""CREATE TABLE cruce.celda_radio (anio smallint, h3 h3index, redcode integer, frac real NOT NULL,
                       PRIMARY KEY (anio, redcode, h3))""")
        for anio in EPOCA:
            sh(cur, f"""
                INSERT INTO cruce.celda_radio
                SELECT {anio}, c.h3, r.redcode,
                       CASE WHEN ST_Within(c.geom, r.geom) THEN 1
                            ELSE ST_Area(ST_Intersection(c.geom, r.geom)) / ST_Area(c.geom) END
                FROM cruce.celda_geom c JOIN censo_{anio}.radios r ON ST_Intersects(c.geom, r.geom)""",
               f'fracciones {anio}')
            # radios sin ninguna celda con peso: todas sus celdas (centro adentro o la del punto interior)
            sh(cur, f"""
                WITH sin AS (SELECT r.redcode, r.geom FROM censo_{anio}.radios r
                             WHERE r.geom IS NOT NULL
                               AND NOT EXISTS (SELECT 1 FROM cruce.celda_radio x WHERE x.anio = {anio} AND x.redcode = r.redcode)),
                cel AS (SELECT s.redcode, s.geom, h3_polygon_to_cells(s.geom, 9) AS h3 FROM sin s
                        UNION
                        SELECT s.redcode, s.geom, h3_lat_lng_to_cell(ST_PointOnSurface(s.geom), 9) FROM sin s)
                INSERT INTO cruce.celda_radio
                SELECT {anio}, c.h3, c.redcode,
                       ST_Area(ST_Intersection(h3_cell_to_boundary_geometry(c.h3), c.geom)) / ST_Area(h3_cell_to_boundary_geometry(c.h3))
                FROM cel c
                ON CONFLICT DO NOTHING""", f'  celdas de radios sin peso {anio}')
        cur.execute('DELETE FROM cruce.celda_radio WHERE frac <= 0')
        cur.execute('CREATE INDEX ON cruce.celda_radio (anio, h3)')
        cur.execute("COMMENT ON TABLE cruce.celda_radio IS 'Parte de cada celda que cae en cada radio de cada año (1 = entera adentro). Celdas con algún peso, más todas las de los radios que no tienen ninguna'")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('ANALYZE cruce.celda_radio')
    conn.autocommit = False


def peso_sql(metodo: str, anio: int) -> str:
    e = EPOCA[anio]
    return {
        'area': 'cr.frac',
        'edificios': 'cr.frac * coalesce(p.edificios, 0)',
        'ghsl': f'cr.frac * coalesce(p.ghsl_{e}, 0)',
        'edificios_ghsl': f'cr.frac * coalesce(p.edificios, 0) * (coalesce(p.construida_{e}, false))::int',
    }[metodo]


# ── 3. validación ─────────────────────────────────────────────────────────────

def validacion(conn) -> pd.DataFrame:
    filas = []
    with conn, conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS cruce.validacion')
        cur.execute("""CREATE TABLE cruce.validacion (anio smallint, metodo text, clase text, radios integer,
                       personas double precision, wape double precision, error_mediano double precision,
                       fracciones_sin_peso integer, PRIMARY KEY (anio, metodo, clase))""")
        for anio in EPOCA:
            for m in METODOS:
                # peso del radio: suma de pesos de sus celdas (área: superficie del radio)
                w = ('r.area_km2' if m == 'area' else
                     f'(SELECT sum({peso_sql(m, anio)}) FROM cruce.celda_radio cr LEFT JOIN cruce.celda_pesos p USING (h3) '
                     f'WHERE cr.anio = {anio} AND cr.redcode = r.redcode)')
                cur.execute(f"""
                    WITH rr AS (
                        SELECT r.redcode, r.redcode / 100 AS fraccion, r.area_km2, r.personas_oficial AS real,
                               coalesce({w}, 0) AS w
                        FROM censo_{anio}.radios r
                        WHERE r.geom IS NOT NULL AND r.personas_oficial IS NOT NULL),
                    fr AS (SELECT fraccion, count(*) AS n, sum(real) AS t, sum(w) AS sw, sum(area_km2) AS sa
                           FROM rr GROUP BY 1 HAVING count(*) > 1),
                    est AS (
                        SELECT rr.*, fr.sw = 0 AS sin_peso,
                               fr.t * CASE WHEN fr.sw > 0 THEN rr.w / fr.sw ELSE rr.area_km2 / fr.sa END AS est
                        FROM rr JOIN fr USING (fraccion)),
                    cl AS (SELECT *, {clase_sql('area_km2')} AS clase FROM est)
                    SELECT clase, count(*), sum(real), sum(abs(est - real)) / nullif(sum(real), 0),
                           percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(est - real) / nullif(real, 0)),
                           count(DISTINCT fraccion) FILTER (WHERE sin_peso)
                    FROM cl GROUP BY ROLLUP (clase)""")
                for clase, n, pers, wape, med, sinp in cur.fetchall():
                    filas.append((anio, m, clase or '0 total', n, pers, wape, med, sinp))
            log(f'  validación {anio} lista')
        cur.executemany('INSERT INTO cruce.validacion VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', filas)
        cur.execute("COMMENT ON TABLE cruce.validacion IS 'Error de cada método al repartir la población de cada fracción censal entre sus radios (fracciones de más de un radio). wape = Σ|est − real| / Σ real; error_mediano = mediana de |est − real| / real por radio'")
    return pd.DataFrame(filas, columns=['anio', 'metodo', 'clase', 'radios', 'personas', 'wape', 'error_mediano', 'fracciones_sin_peso'])


CLASES = ['1 urbano (< 1 km²)', f'2 intermedio (1–{RURAL_KM2} km²)', f'3 rural (> {RURAL_KM2} km²)']


def clase_sql(area: str = 'r.area_km2') -> str:
    return (f"CASE WHEN {area} < 1 THEN '{CLASES[0]}' WHEN {area} <= {RURAL_KM2} THEN '{CLASES[1]}' "
            f"ELSE '{CLASES[2]}' END")


def elegir(v: pd.DataFrame) -> dict[int, dict[str, str]]:
    """Método por año y clase de radio. Total: el WAPE más bajo, o el más simple a menos de 1 punto.
    Una clase usa otro método sólo si ese método le baja el WAPE más de 2 puntos."""
    out = {}
    for anio, g in v.groupby('anio'):
        w = g.pivot(index='clase', columns='metodo', values='wape') * 100
        tot = w.loc['0 total']
        m0 = [m for m in METODOS if tot[m] <= tot.min() + 1][0]
        out[anio] = {}
        for c in CLASES:
            mejor = w.loc[c].idxmin()
            out[anio][c] = mejor if w.loc[c, m0] - w.loc[c, mejor] > 2 else m0
    return out


def informe(v: pd.DataFrame, elegido: dict[int, dict[str, str]]) -> None:
    lin = ['# Validación del cruce radio → celda',
           '',
           f'Generado por `pipeline/hexoma/build_hexoma_cruce.py` el {time.strftime("%Y-%m-%d")}. Método en el docstring del script.',
           '',
           'Para cada año y método se reparte la población de cada fracción censal (de más de un radio) entre sus',
           'radios según la suma de pesos de cada radio, y se compara con la población real del radio.',
           '`WAPE` = Σ|est − real| / Σ real (%); `mediano` = error relativo mediano por radio (%).',
           'Regla fijada antes: gana el WAPE total más bajo; a menos de 1 punto, el más simple (área < edificios < ghsl < edificios_ghsl).',
           'Un tamaño de radio usa otro método sólo si ese método le baja el WAPE más de 2 puntos.',
           '2022: donde gana edificios, se usa el cruce de la web (cada edificio asignado a su radio por punto, `censo_2022.cruce`).',
           '']
    for anio in EPOCA:
        g = v[v.anio == anio].pivot(index='clase', columns='metodo')
        el = '; '.join(f'{c[2:]}: **{m}**' for c, m in elegido[anio].items())
        lin += [f'## {anio} (GHSL {EPOCA[anio]})', '', f'Elegido — {el}', '',
                '| Radios | n | ' + ' | '.join(f'{m} WAPE / mediano' for m in METODOS) + ' |',
                '|---|---|' + '---|' * len(METODOS)]
        for clase in g.index:
            n = int(g.loc[clase, ('radios', METODOS[0])])
            celdas = [f"{g.loc[clase, ('wape', m)] * 100:.1f} / {g.loc[clase, ('error_mediano', m)] * 100:.1f}" for m in METODOS]
            lin.append(f'| {clase[2:]} | {n:,} | ' + ' | '.join(celdas) + ' |')
        sinp = v[(v.anio == anio) & (v.clase == '0 total')].set_index('metodo').fracciones_sin_peso
        lin += ['', 'Fracciones sin peso con cada método (caen a área): ' + ', '.join(f'{m} {int(sinp[m])}' for m in METODOS), '']
    (ROOT / 'pipeline' / 'hexoma' / 'validacion_cruce.md').write_text('\n'.join(lin), encoding='utf-8')


# ── 4. cruce final y confianza ────────────────────────────────────────────────

def cruce_final(conn, elegido: dict[int, dict[str, str]]) -> None:
    cadenas = {'edificios_ghsl': ['edificios_ghsl', 'edificios', 'ghsl', 'area'],
               'edificios': ['edificios', 'ghsl', 'area'], 'ghsl': ['ghsl', 'area'], 'area': ['area']}
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP TABLE IF EXISTS cruce.radio_h3 CASCADE')
        cur.execute("""CREATE TABLE cruce.radio_h3 (anio smallint, h3 h3index, territorio_id smallint, redcode integer,
                       peso double precision NOT NULL, metodo text NOT NULL, PRIMARY KEY (anio, redcode, h3))""")
        for anio, por_clase in elegido.items():
            cur.execute('DROP TABLE IF EXISTS tmp_hecho; CREATE TEMP TABLE tmp_hecho (redcode integer PRIMARY KEY)')
            for clase, m in por_clase.items():
                en_clase = f"r.redcode IN (SELECT redcode FROM censo_{anio}.radios r WHERE {clase_sql()} = '{clase}')"
                if anio == 2022 and m in ('edificios', 'edificios_ghsl'):
                    # el cruce de la web: cada edificio asignado a su radio por punto (renormalizado a 1 por radio)
                    sh(cur, f"""INSERT INTO cruce.radio_h3
                                SELECT 2022, c.h3, c.territorio_id, c.redcode, c.peso / sum(c.peso) OVER (PARTITION BY c.redcode),
                                       CASE c.fuente WHEN 'edificios' THEN 'edificios_punto' ELSE 'area' END
                                FROM censo_2022.cruce c JOIN censo_2022.radios r USING (redcode)
                                WHERE {en_clase}
                                ON CONFLICT DO NOTHING""", f'2022 {clase[2:]}: edificios por punto (censo_2022.cruce)')
                    cur.execute("INSERT INTO tmp_hecho SELECT DISTINCT redcode FROM cruce.radio_h3 WHERE anio = 2022 ON CONFLICT DO NOTHING")
                    m = 'edificios'  # los radios de la clase que no estén en el cruce de la web siguen la cadena
                for mm in cadenas[m]:
                    sh(cur, f"""
                        WITH w AS (
                            SELECT cr.redcode, cr.h3, {peso_sql(mm, anio)} AS w
                            FROM cruce.celda_radio cr LEFT JOIN cruce.celda_pesos p USING (h3)
                            JOIN censo_{anio}.radios r ON r.redcode = cr.redcode
                            WHERE cr.anio = {anio} AND {en_clase}
                              AND cr.redcode NOT IN (SELECT redcode FROM tmp_hecho)),
                        s AS (SELECT redcode, sum(w) AS sw FROM w GROUP BY 1 HAVING sum(w) > 0)
                        INSERT INTO cruce.radio_h3
                        SELECT {anio}, w.h3, r.territorio_id, w.redcode, w.w / s.sw, '{mm}'
                        FROM w JOIN s USING (redcode) JOIN censo_{anio}.radios r USING (redcode)
                        WHERE w.w > 0""", f'{anio} {clase[2:]}: {mm}')
                    cur.execute(f"INSERT INTO tmp_hecho SELECT DISTINCT redcode FROM cruce.radio_h3 WHERE anio = {anio} ON CONFLICT DO NOTHING")
        cur.execute('CREATE INDEX ON cruce.radio_h3 (anio, h3)')
        cur.execute("COMMENT ON TABLE cruce.radio_h3 IS 'Cruce oficial radio → celda por año. peso = parte del radio que va a la celda (suma 1 por radio). metodo = el que se usó para ese radio (el elegido por cruce.validacion, o el siguiente de la cadena si el radio no tenía peso). 2022: edificios_punto = el cruce de la web (edificio asignado a su radio por punto)'")
        # confianza por celda y año
        cur.execute('DROP TABLE IF EXISTS cruce.celda_confianza')
        sh(cur, f"""
            CREATE TABLE cruce.celda_confianza AS
            SELECT x.anio, x.h3, count(*)::smallint AS n_radios,
                   sum(x.peso * coalesce(r.personas_oficial, 0))::real AS personas_est,
                   (sum(x.peso * coalesce(r.personas_oficial, 0)) FILTER (WHERE r.area_km2 > {RURAL_KM2})
                    / nullif(sum(x.peso * coalesce(r.personas_oficial, 0)), 0))::real AS parte_radios_grandes,
                   bool_or(x.metodo = 'area') AS usa_area,
                   bool_or(coalesce(r.factor_muestra, 1) > 1.5) AS usa_muestra
            FROM cruce.radio_h3 x
            JOIN (SELECT 1991 AS anio, redcode, personas_oficial, area_km2, factor_muestra FROM censo_1991.radios
                  UNION ALL SELECT 2001, redcode, personas_oficial, area_km2, factor_muestra FROM censo_2001.radios
                  UNION ALL SELECT 2010, redcode, personas_oficial, area_km2, factor_muestra FROM censo_2010.radios
                  UNION ALL SELECT 2022, redcode, personas_oficial, area_km2, factor_muestra FROM censo_2022.radios) r
                 USING (anio, redcode)
            GROUP BY 1, 2""", 'confianza')
        cur.execute('ALTER TABLE cruce.celda_confianza ADD PRIMARY KEY (anio, h3)')
        cur.execute(f"COMMENT ON TABLE cruce.celda_confianza IS 'Por celda y año: radios que aportan, personas estimadas, parte de esas personas que viene de radios de más de {RURAL_KM2} km² (donde la ubicación dentro del radio es una estimación gruesa), si algún radio se repartió por área y si alguno tiene muestra (factor > 1,5: tasas ruidosas)'")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('ANALYZE cruce.radio_h3; ANALYZE cruce.celda_confianza')
    conn.autocommit = False


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""SELECT anio, count(DISTINCT redcode), count(*), min(s), max(s) FROM (
                         SELECT anio, redcode, sum(peso) AS s FROM cruce.radio_h3 GROUP BY 1, 2) x GROUP BY 1 ORDER BY 1""")
        for a, n, _, mn, mx in cur.fetchall():
            log(f'  {a}: {n:,} radios en el cruce, Σ peso por radio entre {mn:.6f} y {mx:.6f}')
        for anio in EPOCA:
            cur.execute(f"""SELECT count(*), sum(personas_oficial)::bigint FROM censo_{anio}.radios r
                            WHERE personas_oficial > 0 AND NOT EXISTS (SELECT 1 FROM cruce.radio_h3 x WHERE x.anio = {anio} AND x.redcode = r.redcode)""")
            n, p = cur.fetchone()
            cur.execute(f"""SELECT sum(x.peso * r.personas_oficial)::bigint, (SELECT sum(personas_oficial)::bigint FROM censo_{anio}.radios)
                            FROM cruce.radio_h3 x JOIN censo_{anio}.radios r USING (redcode) WHERE x.anio = {anio}""")
            en_celdas, total = cur.fetchone()
            log(f'  {anio}: personas en celdas {en_celdas:,} de {total:,}; radios con población fuera del cruce: {n} ({p or 0:,} personas, sin geometría)')


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--desde', choices=['pesos', 'fracciones', 'validacion', 'final'], default='pesos')
    args = ap.parse_args()
    pasos = ['pesos', 'fracciones', 'validacion', 'final']
    hacer = pasos[pasos.index(args.desde):]
    t0 = time.time()
    conn = connect(DB)
    if 'pesos' in hacer:
        pesos(conn)
    if 'fracciones' in hacer:
        fracciones(conn)
    if 'validacion' in hacer:
        v = validacion(conn)
    else:
        v = pd.read_sql('SELECT * FROM cruce.validacion', conn)
    elegido = elegir(v)
    for a, pc in elegido.items():
        log(f'  elegido {a}: ' + ', '.join(f'{c[2:]} {m}' for c, m in pc.items()))
    informe(v, elegido)
    if 'final' in hacer:
        cruce_final(conn, elegido)
        verificar(conn)
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
