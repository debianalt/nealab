"""Compare the live fact sheet with the Hexoma (distribuciones + rango_*), layer by layer.

1) Capture the sheet with Playwright:
     PYTHONIOENCODING=utf-8 python scripts/verify_ficha.py https://www.spatia.ar <h3> [<h3> ...] > ficha.txt
2) Compare (from neahub/):
     PYTHONIOENCODING=utf-8 python pipeline/hexoma/verificar_ficha.py ficha.txt

For each card checks that the department median, mean and the «más alto / más bajo /
igual que N de cada 100» sentence (HexFicha.svelte words(): floor of below/above) match
what the Hexoma computes for the layer's primary variable. Prints «N OK, M DIFF».
"""
import json, re, sys
from decimal import Decimal, ROUND_HALF_UP
import psycopg2
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
reg = json.load(open(ROOT / 'pipeline' / 'output' / 'hexoma' / 'registry.json', encoding='utf-8'))
c = psycopg2.connect('postgresql://postgres@localhost:5432/hexoma'); c.set_client_encoding('UTF8'); cur = c.cursor()

def fmt(v):
    a = abs(v); d = 0 if a >= 100 else 1 if (a >= 0.1 or a == 0) else 2
    q = Decimal(v).quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
    s = f'{q:,.{d}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
    if ',' in s: s = s.rstrip('0').rstrip(',')
    return s

def words(lt, eq):
    import math
    below, above, e = math.floor(lt), math.floor(100 - lt - eq), round(eq)
    if eq >= 20:
        h = f'Igual que {e} de cada 100'
        if below >= 1 and below >= above: return [h, f'más alto que {below}']
        if above >= 1: return [h, f'más bajo que {above}']
        return [h]
    return [f'Más alto que {below} de cada 100'] if below >= above else [f'Más bajo que {above} de cada 100']

live = {}
cell = None
for line in open(sys.argv[1], encoding='utf-8'):
    m = re.match(r'\?h3=(\w+)', line)
    if m: cell = m.group(1); live[cell] = []; continue
    if cell and line.startswith('    ') and not line.strip().startswith('page errors'):
        live[cell].append(line.strip())

ok = bad = 0
for h, cards in live.items():
    for layer, card in zip(reg['layers'], cards):
        lid = layer['id']
        v = next((x for x in layer['variables'] if layer['primary'] in (x['col'], x['rawCol'])), None)
        scope = 'dep' if layer['perDepartment'] else 'prov'
        cur.execute(f'SELECT r."{v["col"]}__{scope}_lt", r."{v["col"]}__{scope}_eq", c.depto_id, c.territorio_id '
                    f'FROM capa_{lid} c LEFT JOIN rango_{lid} r USING (h3, territorio_id) WHERE c.h3 = %s AND c.principal', (h,))
        row = cur.fetchone()
        if 'sin dato' in card or 'sin población' in card:
            st = 'OK' if (row is None or row[0] is None) else 'DIFF(ficha sin dato, hexoma tiene rango)'
            ok += st == 'OK'; bad += st != 'OK'
            if st != 'OK': print(h, lid, st)
            continue
        if row is None or row[0] is None:
            print(h, lid, 'DIFF: hexoma sin rango'); bad += 1; continue
        lt, eq, did, tid = row[0] / 100, row[1] / 100, row[2], row[3]
        aid = str(did) if scope == 'dep' else None
        if scope == 'dep':
            cur.execute("SELECT p10,p50,p90,media FROM distribuciones WHERE capa=%s AND variable=%s AND ambito='departamento' AND ambito_id=%s", (lid, v['col'], aid))
        else:
            cur.execute("SELECT p10,p50,p90,media FROM distribuciones d JOIN territorios t ON t.territorio=d.ambito_id WHERE capa=%s AND variable=%s AND ambito='provincia' AND t.territorio_id=%s", (lid, v['col'], tid))
        p10, p50, p90, mean = cur.fetchone()
        want = [f'mediana {fmt(p50)}', f'promedio {fmt(mean)}'] + words(lt, eq)
        miss = [w for w in want if w not in card]
        if miss:
            bad += 1; print(h, lid, 'DIFF', miss, '|', card[:160])
        else:
            ok += 1
print(f'{ok} OK, {bad} DIFF')
