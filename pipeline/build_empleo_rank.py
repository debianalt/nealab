"""
Ranking de actividad económica por departamento: ocupados (cantidad y % de la provincia) y desocupación (Censo 2022).

La tasa de empleo (ocupados / población de 14+) varía poco entre departamentos (Misiones: 52-60 %) y ordena por
diferencias chicas; además el censo cuenta como ocupado a quien trabajó al menos una hora la semana anterior, también
ayudando en una actividad familiar con o sin pago, así que los departamentos rurales no salen abajo. Para la pregunta
de la capa («¿dónde se concentra la actividad económica?») la lectura es cuántos ocupados hay y qué parte de la
provincia son. La desocupación (desocupados / activos) discrimina más que la tasa de empleo.

Fuente: censo_2022.censo_tasaactividad_2022 (radios, una fila por radio con sexo = 1 = total), sumada por departamento
INDEC (radios_geom.dpto), la misma partición de Spatia. Son conteos por radio, no estimaciones por hexágono: por
hexágono no hay conteo de ocupados (la tabla del hexágono sigue con la tasa).

Escribe en el `rank` de cada departamento del resumen de economic_activity de los 4 territorios AR:
  ocupados_n          personas ocupadas
  ocupados_prov_pct   % de los ocupados de la provincia
  desocup_pct         desocupados / activos × 100
(empleo_pct, la tasa, la sigue calculando build_layer_ranks.py desde los hexágonos.)

    python pipeline/build_empleo_rank.py
"""
import json
import os
import sys

import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from build_univ25_rank import norm   # nombres de departamento: acentos, «1º», abreviaturas de Misiones

DATA = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
PG_CENSUS = 'dbname=posadas user=postgres'
TERRITORIES = ('misiones', 'corrientes', 'chaco', 'formosa')


def radios(tid: str) -> dict[str, dict]:
    cur = psycopg2.connect(PG_CENSUS).cursor()
    cur.execute("""SELECT g.dpto, SUM(a.ocupados), SUM(a.desocupado), SUM(a.activos), SUM(a.total_pobl)
                   FROM censo_2022.censo_tasaactividad_2022 a JOIN censo_2022.radios_geom g USING (redcode)
                   WHERE g.prov ILIKE %s AND a.sexo = 1 GROUP BY 1""", (tid,))
    out = {}
    for d, oc, de, ac, p14 in cur.fetchall():
        oc, de, ac, p14 = (float(x or 0) for x in (oc, de, ac, p14))
        if abs(oc + de - ac) > 0.001 * ac:
            raise SystemExit(f'{tid}/{d}: ocupados + desocupados ({oc + de:.0f}) ≠ activos ({ac:.0f})')
        out[norm(d)] = {'name': d, 'oc': oc, 'de': de, 'ac': ac, 'p14': p14}
    return out


def main() -> None:
    for tid in TERRITORIES:
        rows = radios(tid)
        total = sum(r['oc'] for r in rows.values())
        fname = 'sat_economic_activity_dept_summary.json' if tid == 'misiones' else f'{tid}_sat_economic_activity_summary.json'
        path = os.path.join(DATA, fname)
        with open(path, encoding='utf-8') as f:
            summary = json.load(f)
        missing = [d['dpto'] for d in summary['departments'] if norm(d['dpto']) not in rows]
        if missing:
            raise SystemExit(f'{tid}: sin radios para {missing}')
        for d in summary['departments']:
            r = rows[norm(d['dpto'])]
            d['rank'] = {**(d.get('rank') or {}),
                         'ocupados_n': int(round(r['oc'])),
                         'ocupados_prov_pct': round(100 * r['oc'] / total, 1),
                         'desocup_pct': round(100 * r['de'] / r['ac'], 1)}
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        prov_emp = 100 * total / sum(r['p14'] for r in rows.values())
        prov_des = 100 * sum(r['de'] for r in rows.values()) / sum(r['ac'] for r in rows.values())
        print(f'\n{tid}: {total:,.0f} ocupados · empleo {prov_emp:.1f} % · desocupación {prov_des:.1f} %')
        for d in sorted(summary['departments'], key=lambda d: -d['rank']['ocupados_n']):
            k = d['rank']
            emp_radio = 100 * rows[norm(d['dpto'])]['oc'] / rows[norm(d['dpto'])]['p14']
            print(f"  {d['dpto'][:26]:26s} {k['ocupados_n']:>8,} {k['ocupados_prov_pct']:5.1f} %   "
                  f"desoc {k['desocup_pct']:4.1f} %   empleo hex {k.get('empleo_pct')} / radios {emp_radio:.1f}")


if __name__ == '__main__':
    main()
