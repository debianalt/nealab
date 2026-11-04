"""
Ranking «universitario completo» por departamento sobre la población de 25 años y más (Censo 2022, INDEC).

Por qué: la tabla por radio censal (censo_niveducativo_2022) sólo separa menores de 18 y 18 y más, así que el mapa por
hexágono usa 18+. Entre los 18 y los 24 casi nadie pudo terminar una carrera: con 18+ los departamentos de población
joven aparecen con menos universitarios de los que tienen. La convención (UNESCO/OCDE) para nivel educativo alcanzado
es 25+.

Método (misma definición que el mapa):
  numerador   = graduados universitarios de los radios del departamento (p18a_unive = máximo nivel de instrucción
                «universitario completo», MNI 8: incluye a quien ya tiene título y hoy cursa otra cosa)
                × (1 − s), donde s = parte de esos graduados que tiene 18 a 24 años.
  denominador = población de 25 años y más en viviendas particulares del departamento.
  s y la población 25+ salen de los cuadros por departamento que INDEC publica:
    c2022_<prov>_educacion_c3_<n>.xlsx  Cuadro 3: población en viviendas particulares por edad (col. 2) y, de quienes
                                        asistieron y ya no asisten, máximo nivel alcanzado y completitud.
    c2022_<prov>_educacion_c2_<n>.xlsx  Cuadro 2: quienes asisten, por nivel (col. 10 = posgrado).
  graduados por edad en los cuadros = universitario completo + posgrado (c3) + asiste a posgrado (c2). Los cuadros no
  sirven como numerador directo: no dicen cuántos de los que hoy cursan grado o terciario ya tienen un título, y MNI 8
  sí los cuenta (Capital, Misiones: 26.948 en los cuadros contra 32.936 en los radios).
  Control: la población de 18+ de los cuadros es la misma que p_18a de los radios (Capital: 279.605 en ambos).

Escribe rank.univ25_pct en los resúmenes de education_capital y economic_activity de los 4 territorios AR
(build_layer_ranks.py ya no calcula univ_pct y conserva los campos que no son suyos).

    python pipeline/build_univ25_rank.py
"""
import json
import os
import re
import unicodedata
import urllib.request

import openpyxl
import psycopg2

ROOT = os.path.dirname(__file__)
CACHE = os.path.join(ROOT, 'data', 'indec_educacion_2022')
DATA = os.path.join(ROOT, '..', 'src', 'lib', 'data')
FTP = 'https://www.indec.gob.ar/ftp/cuadros/poblacion/'
PG_CENSUS = 'dbname=posadas user=postgres'
PROV = {'misiones': ('misiones', 14), 'corrientes': ('corrientes', 7), 'chaco': ('chaco', 4), 'formosa': ('formosa', 9)}
LAYERS = ('education_capital', 'economic_activity')
# Spatia abrevia tres nombres de Misiones
ALIAS = {'g m belgrano': 'general manuel belgrano', 'l n alem': 'leandro n alem',
         'lg san martin': 'libertador general san martin', 'l g san martin': 'libertador general san martin'}
C3_POP, C3_UNIV_COMPLETO, C3_POSGRADO = 2, 22, 23
C2_POSGRADO = 10
POP_TOLERANCE = 0.005   # población 18+ cuadros vs radios


def norm(s: str) -> str:
    s = s.replace('º', '').replace('°', '')   # «1º de Mayo» (cuadros) = «1° de Mayo» (radios)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode().lower()
    s = re.sub(r'[^a-z0-9 ]', ' ', s.replace("'", ''))
    s = re.sub(r'\s+', ' ', s).strip()
    return ALIAS.get(s, s)


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0   # '-', '///', None


def fetch(name: str) -> str:
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, name)
    if not os.path.exists(path):
        req = urllib.request.Request(FTP + name, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=120) as r, open(path, 'wb') as f:
            f.write(r.read())
    with open(path, 'rb') as f:
        if f.read(2) != b'PK':
            raise SystemExit(f'{name}: no es un xlsx (INDEC cambió la ruta)')
    return path


def by_age(ws, cols: list[int]) -> dict[tuple[int, int], list[float]]:
    """Bloque «Total» (ambos sexos) sin doble conteo: edades simples hasta 29, grupos desde 30-34, 90 y más.
    Devuelve {(edad_desde, edad_hasta): [valores de cols]}."""
    out, started = {}, False
    for row in ws.iter_rows(min_row=4, values_only=True):
        head = str(row[0] or '').strip()
        if head == 'Total':
            started = True
            continue
        if not started:
            continue
        if head:            # empieza el bloque Varón/Mujer
            break
        a = str(row[1] or '').strip()
        if re.fullmatch(r'\d+', a) and int(a) <= 29:
            key = (int(a), int(a))
        elif re.fullmatch(r'\d+-\d+', a) and int(a.split('-')[0]) >= 30:
            key = tuple(int(x) for x in a.split('-'))
        elif a.startswith('90'):
            key = (90, 200)
        else:
            continue
        out[key] = [num(row[c]) for c in cols]
    return out


def dept_name(ws) -> str:
    t = str(next(ws.iter_rows(min_row=2, max_row=2, values_only=True))[0]).replace('\xa0', ' ')
    m = re.search(r'departamento (.+?)\. (?:Poblaci|Total)', t)
    return m.group(1) if m else ''


def cuadros(tid: str) -> dict[str, dict]:
    slug, n = PROV[tid]
    c3 = openpyxl.load_workbook(fetch(f'c2022_{slug}_educacion_c3_{n}.xlsx'), read_only=True)
    c2 = openpyxl.load_workbook(fetch(f'c2022_{slug}_educacion_c2_{n}.xlsx'), read_only=True)
    out = {}
    for sn in c3.sheetnames[3:]:   # Carátula, Índice, cuadro provincial; después un cuadro por departamento
        ws3, ws2 = c3[sn], c2[sn.replace('Cuadro 3.', 'Cuadro 2.')]
        name = dept_name(ws3)
        if not name or name != dept_name(ws2):
            raise SystemExit(f'{tid}: {sn} no coincide entre cuadros ({name!r} / {dept_name(ws2)!r})')
        a3 = by_age(ws3, [C3_POP, C3_UNIV_COMPLETO, C3_POSGRADO])
        a2 = by_age(ws2, [C2_POSGRADO])
        grad = lambda k: a3[k][1] + a3[k][2] + a2.get(k, [0])[0]
        out[norm(name)] = {
            'name': name,
            'pop18': sum(v[0] for k, v in a3.items() if k[0] >= 18),
            'pop25': sum(v[0] for k, v in a3.items() if k[0] >= 25),
            'grad18': sum(grad(k) for k in a3 if k[0] >= 18),
            'grad18_24': sum(grad(k) for k in a3 if 18 <= k[0] <= 24),
        }
    return out


def radios(tid: str) -> dict[str, dict]:
    cur = psycopg2.connect(PG_CENSUS).cursor()
    cur.execute("""SELECT g.dpto, SUM(n.p_18a), SUM(n.p18a_unive)
                   FROM censo_2022.censo_niveducativo_2022 n JOIN censo_2022.radios_geom g USING (redcode)
                   WHERE g.prov ILIKE %s GROUP BY 1""", (tid,))
    return {norm(d): {'p18': float(p), 'unive': float(u)} for d, p, u in cur.fetchall()}


def main() -> None:
    for tid in PROV:
        cq, rd = cuadros(tid), radios(tid)
        rows = {}
        for k, c in cq.items():
            r = rd.get(k)
            if r is None:
                raise SystemExit(f'{tid}: {c["name"]} sin radios')
            if abs(r['p18'] - c['pop18']) > POP_TOLERANCE * c['pop18']:
                raise SystemExit(f'{tid}/{c["name"]}: población 18+ cuadros {c["pop18"]:.0f} vs radios {r["p18"]:.0f}')
            s = c['grad18_24'] / c['grad18'] if c['grad18'] else 0.0
            rows[k] = {**c, 's': s, 'pct': 100 * r['unive'] * (1 - s) / c['pop25'], 'pct18': 100 * r['unive'] / r['p18']}
        for layer in LAYERS:
            fname = f'sat_{layer}_dept_summary.json' if tid == 'misiones' else f'{tid}_sat_{layer}_summary.json'
            path = os.path.join(DATA, fname)
            with open(path, encoding='utf-8') as f:
                summary = json.load(f)
            missing = [d['dpto'] for d in summary['departments'] if norm(d['dpto']) not in rows]
            if missing:
                raise SystemExit(f'{tid}/{layer}: sin cuadro INDEC para {missing}')
            for d in summary['departments']:
                rank = {k: v for k, v in (d.get('rank') or {}).items() if k != 'univ_pct'}   # 18+, retirado
                d['rank'] = {**rank, 'univ25_pct': round(rows[norm(d['dpto'])]['pct'], 1)}
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f'\n{tid}: {len(rows)} departamentos · universitario completo, % de 25+ vs % de 18+ (s = graduados de 18-24)')
        for r in sorted(rows.values(), key=lambda r: -r['pct']):
            print(f"  {r['name'][:30]:30s} 25+ {r['pct']:5.1f}   18+ {r['pct18']:5.1f}   s {100 * r['s']:4.1f} %")


if __name__ == '__main__':
    main()
