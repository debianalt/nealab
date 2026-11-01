"""Hexoma — datos externos por radio censal (esquema `externos`), con su procedencia.

Un externo es un dato que no es el censo crudo: una estimación de un tercero sobre la grilla de
radios de un censo. Se guarda en su unidad de origen, tal cual, con una fila en `externos.fuentes`
que dice quién lo hizo, cómo, dónde se publicó y con qué licencia. Pasa a las celdas con el cruce
del año de su grilla (esquema `cruce`, etapa 2).

Hoy:
    uca_inseguridad_alimentaria_2010   riesgo de inseguridad alimentaria por radio 2010 (UCA-ODSA),
                                       importado en la base local `posadas`
                                       (censo_2010.inseg_alimentaria_2010)

Quedó afuera el «índice de exclusión» de posadas.censo_2010.exclusion_2010 (incidencia 1–6,
concentración 1–3): no se encontró quién lo hizo ni con qué método (búsqueda del 1-nov-2026 en
Poblaciones y en la web). Sin procedencia no entra.

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_externos.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

PROVS = ('54', '18', '22', '34')
FUENTES = [
    dict(
        tabla='uca_inseguridad_alimentaria_2010',
        titulo='Estimación y georreferenciación de la probabilidad de padecer inseguridad alimentaria',
        autores='Márquez, Ana Lourdes; Salvia, Agustín',
        institucion='Observatorio de la Deuda Social Argentina, Universidad Católica Argentina (UCA)',
        anio_publicacion=2017,
        url='https://repositorio.uca.edu.ar/bitstream/123456789/21765/1/estimacion-y-georreferenciacion-de-la-probabilidad-de-padecer-inseguridad-alimentaria.pdf',
        licencia='No declarada en la tabla importada; citar el informe',
        grilla='radios censales 2010',
        metodo=('Modelo sobre la EDSA-UCA (hogares que redujeron o salteaban comidas por falta de dinero en los '
                'últimos 12 meses) aplicado a indicadores del Censo 2010 por hogar (heladera, hacinamiento, '
                'teléfono, baño y otros), reducido a un índice 0–100 y resumido por radio en intensidad (ia, '
                'promedio del índice) y prevalencia por deciles. Al estar armado con variables del censo 2010, '
                'no es independiente del NBI ni de los indicadores del censo de ese año: no usarlo como variable '
                'explicada por ellos.'),
        importado_de='posadas.censo_2010.inseg_alimentaria_2010 (tabla en la base local, origen según el autor: Poblaciones)',
    ),
]


def main() -> None:
    src = connect('posadas')
    with src.cursor() as cur:
        cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = 'censo_2010' AND table_name = 'inseg_alimentaria_2010'
                       ORDER BY ordinal_position""")
        skip = {'geometry', 'idprov', 'prov', 'iddpto', 'dpto', 'redcode', 'radios_pob', 'radios_hog',
                'radios_lat', 'radios_lon', 'radios_sup', 'link', 'provincia_', 'depto', 'localidad', 'municipio'}
        cols = [(c, t) for c, t in cur.fetchall() if c not in skip]
        cur.execute(f"""SELECT link::integer, depto, localidad, municipio, {', '.join(c for c, _ in cols)}
                        FROM censo_2010.inseg_alimentaria_2010 WHERE left(link, 2) IN %s""", (PROVS,))
        rows = cur.fetchall()
    src.close()
    log(f'  uca_inseguridad_alimentaria_2010: {len(rows):,} radios del NEA, {len(cols)} columnas')

    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute("SET client_min_messages = warning")
        cur.execute("DROP SCHEMA IF EXISTS externos CASCADE")
        cur.execute("CREATE SCHEMA externos")
        cur.execute("COMMENT ON SCHEMA externos IS 'Datos de terceros sobre la grilla de radios de un censo, tal cual, con su procedencia en externos.fuentes'")
        cur.execute("""CREATE TABLE externos.fuentes (tabla text PRIMARY KEY, titulo text, autores text, institucion text,
                       anio_publicacion integer, url text, licencia text, grilla text, metodo text, importado_de text)""")
        for f in FUENTES:
            cur.execute(f"INSERT INTO externos.fuentes ({', '.join(f)}) VALUES ({', '.join(['%s'] * len(f))})",
                        list(f.values()))
        cur.execute("CREATE TABLE externos.uca_inseguridad_alimentaria_2010 (redcode integer PRIMARY KEY, "
                    "departamento text, localidad text, municipio text, "
                    + ', '.join(f'{c} double precision' for c, _ in cols) + ')')
        cur.executemany(f"INSERT INTO externos.uca_inseguridad_alimentaria_2010 VALUES ({', '.join(['%s'] * (4 + len(cols)))})",
                        rows)
        cur.execute("COMMENT ON TABLE externos.uca_inseguridad_alimentaria_2010 IS "
                    "'Riesgo de inseguridad alimentaria por radio 2010 (UCA-ODSA, Márquez y Salvia 2017). redcode = radio 2010 "
                    "(censo_2010.radios). ia = intensidad (promedio del índice 0–100 en el radio); p_dec*/n_dec* = hogares por decil "
                    "de riesgo; p_* = porcentajes de los indicadores censales del modelo. Ver externos.fuentes'")
        cur.execute("""SELECT count(*), count(r.redcode) FROM externos.uca_inseguridad_alimentaria_2010 u
                       LEFT JOIN censo_2010.radios r USING (redcode)""")
        n, m = cur.fetchone()
        log(f'  radios con su radio 2010 en censo_2010.radios: {m:,} de {n:,}')
    conn.close()


if __name__ == '__main__':
    main()
