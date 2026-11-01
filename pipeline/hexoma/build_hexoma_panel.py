"""Hexoma — panel censal 1991–2022 por radio y por celda H3 (esquema `panel`).

Toma de censo_<año>.indicadores sólo los indicadores que tienen la misma definición en los años en
que aparecen (censo_specs.PANEL, cada uno con su nota de comparabilidad) y los pasa a las celdas
con el cruce oficial de cada año (cruce.radio_h3). La celda es la geografía que no cambia entre
censos: los radios se parten, se fusionan y cambian de código, la celda no.

    panel.definiciones   indicador × año: columna de origen, rótulo y nota
    panel.radio          (anio, redcode, indicador, num, den) en la grilla de radios de cada año
    panel.v_h3           materializada, una fila por (anio, h3, territorio): <ind>_num y <ind>_den
                         (o <ind> para los totales) = Σ peso × conteo del radio, más la confianza
                         de la celda (cruce.celda_confianza)
    panel.v_h3_tasas     vista: las tasas (%) de v_h3

Para tasas de un conjunto de celdas sumar _num y _den y dividir al final. Los indicadores que un
año no tiene quedan NULL ese año.

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_panel.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from censo_specs import PANEL, TODOS  # noqa: E402


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS panel CASCADE')
        cur.execute('CREATE SCHEMA panel')
        cur.execute("COMMENT ON SCHEMA panel IS 'Panel censal 1991-2022: indicadores de definición comparable, por radio de cada año y por celda H3 (pipeline/hexoma/build_hexoma_panel.py)'")
        cur.execute("""CREATE TABLE panel.definiciones (indicador text, tipo text, anio smallint, columna text,
                       rotulo text, nota text, PRIMARY KEY (indicador, anio))""")
        for ind, tipo, cols, rot, nota in PANEL:
            for anio, col in cols.items():
                cur.execute('INSERT INTO panel.definiciones VALUES (%s, %s, %s, %s, %s, %s)',
                            (ind, tipo, anio, col, rot, nota))

        # panel por radio, en formato largo
        partes = []
        for ind, tipo, cols, *_ in PANEL:
            for anio, col in cols.items():
                num, den = (f'{col}_num', f'{col}_den') if tipo == 'pct' else (col, 'NULL::double precision')
                partes.append(f"SELECT {anio}::smallint AS anio, i.redcode, r.territorio_id, '{ind}'::text AS indicador, "
                              f"i.{num} AS num, {den} AS den FROM censo_{anio}.indicadores i "
                              f"JOIN censo_{anio}.radios r USING (redcode) WHERE i.{num} IS NOT NULL")
        cur.execute('CREATE TABLE panel.radio AS ' + ' UNION ALL '.join(partes))
        cur.execute('ALTER TABLE panel.radio ADD PRIMARY KEY (anio, redcode, indicador)')
        log(f'  panel.radio: {cur.rowcount if cur.rowcount >= 0 else ""} filas')
        cur.execute("COMMENT ON TABLE panel.radio IS 'Indicadores del panel en la grilla de radios de cada año (los radios no son comparables entre años: para comparar, usar panel.v_h3). num/den calibrados; den NULL en los totales'")

        # panel por celda: una consulta por año, mismas columnas (NULL donde el año no tiene el indicador)
        cols_out = []
        for ind, tipo, *_ in PANEL:
            cols_out += [f'{ind}_num', f'{ind}_den'] if tipo == 'pct' else [ind]
        selects = []
        for anio in TODOS:
            exprs = []
            for ind, tipo, cols, *_ in PANEL:
                col = cols.get(anio)
                if tipo == 'pct':
                    exprs += [f'sum(x.peso * i.{col}_num)::real AS {ind}_num' if col else f'NULL::real AS {ind}_num',
                              f'sum(x.peso * i.{col}_den)::real AS {ind}_den' if col else f'NULL::real AS {ind}_den']
                else:
                    exprs.append(f'sum(x.peso * i.{col})::real AS {ind}' if col else f'NULL::real AS {ind}')
            selects.append(f"""
                SELECT x.anio, x.h3, x.territorio_id, count(*)::smallint AS n_radios, {', '.join(exprs)}
                FROM cruce.radio_h3 x JOIN censo_{anio}.indicadores i USING (redcode)
                WHERE x.anio = {anio}
                GROUP BY 1, 2, 3""")
        t1 = time.time()
        cur.execute(f"""CREATE MATERIALIZED VIEW panel.v_h3 AS
            SELECT s.anio, s.h3, s.territorio_id, t.territorio, s.n_radios,
                   c.personas_est, c.parte_radios_grandes, c.usa_area, c.usa_muestra,
                   {', '.join('s.' + c for c in cols_out)}
            FROM ({' UNION ALL '.join(selects)}) s
            JOIN territorios t USING (territorio_id)
            LEFT JOIN cruce.celda_confianza c USING (anio, h3)""")
        log(f'  panel.v_h3: {time.time() - t1:.0f} s')
        cur.execute('CREATE UNIQUE INDEX ON panel.v_h3 (anio, h3, territorio_id)')
        cur.execute('CREATE INDEX ON panel.v_h3 (h3)')
        tasas = [f"round((100.0 * {ind}_num / nullif({ind}_den, 0))::numeric, 2) AS {ind}"
                 for ind, tipo, *_ in PANEL if tipo == 'pct']
        totales = [ind for ind, tipo, *_ in PANEL if tipo == 'total']
        cur.execute(f"""CREATE VIEW panel.v_h3_tasas AS
            SELECT anio, h3, territorio_id, territorio, n_radios, personas_est, parte_radios_grandes, usa_area, usa_muestra,
                   {', '.join(totales)}, {', '.join(tasas)}
            FROM panel.v_h3""")
        cur.execute("COMMENT ON MATERIALIZED VIEW panel.v_h3 IS 'Panel censal por celda y año: Σ peso × conteo de cada radio (cruce.radio_h3). Sumar _num y _den y dividir al final. personas_est, parte_radios_grandes, usa_area, usa_muestra: cruce.celda_confianza'")
        for ind, tipo, cols, rot, nota in PANEL:
            anios = ', '.join(str(a) for a in sorted(cols))
            txt = f'{rot}. Años: {anios}.' + (f' {nota}' if nota else '')
            if tipo == 'pct':
                cur.execute(f"COMMENT ON COLUMN panel.v_h3.{ind}_num IS {q('Numerador: ' + txt)}")
                cur.execute(f"COMMENT ON COLUMN panel.v_h3.{ind}_den IS {q('Denominador: ' + txt)}")
                cur.execute(f"COMMENT ON COLUMN panel.v_h3_tasas.{ind} IS {q('% ' + txt)}")
            else:
                cur.execute(f"COMMENT ON COLUMN panel.v_h3.{ind} IS {q(txt)}")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('ANALYZE panel.radio; ANALYZE panel.v_h3')
        # verificación: la celda conserva el total provincial de cada año
        cur.execute("""
            WITH c AS (SELECT anio, territorio, sum(personas) AS p, sum(nbi_num) AS n, sum(nbi_den) AS d
                       FROM panel.v_h3 GROUP BY 1, 2),
            r AS (SELECT p.anio, t.territorio, sum(p.num) FILTER (WHERE indicador = 'personas') AS p,
                         sum(p.num) FILTER (WHERE indicador = 'nbi') AS n, sum(p.den) FILTER (WHERE indicador = 'nbi') AS d
                  FROM panel.radio p JOIN territorios t USING (territorio_id) GROUP BY 1, 2)
            SELECT anio, territorio, round(r.p), round(c.p), round((100 * r.n / r.d)::numeric, 2), round((100 * c.n / c.d)::numeric, 2)
            FROM r JOIN c USING (anio, territorio) ORDER BY 2, 1""")
        log('  verificación: año | territorio | personas radios | personas celdas | NBI % radios | NBI % celdas')
        for row in cur.fetchall():
            log('    ' + ' | '.join(f'{x:,}' if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x) for x in row))
        cur.execute("SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'panel'")
        log('  esquema panel:', cur.fetchone()[0])
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
