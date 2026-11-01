"""Hexoma A4 — contrasta la aptitud por cultivo con lo que se cultiva de verdad.

1. Lift contra MapBiomas Col. 3 (2025): de las celdas cultivadas (≥ 50 % de la superficie), qué parte cae en S1 y en
   S1+S2 (clase mediana de aptitud.resumen), frente a la parte de todas las celdas del ámbito. Lift = cociente.
     extensivos (soja, maíz, algodón, girasol)  cultivo temporal, NEA
     yerba, té, cítricos                        cultivo perenne, Misiones y Corrientes
     pasturas                                   pastura implantada, NEA
2. Departamentos contra el MAGyP: correlación (Pearson y Spearman) entre la superficie S1 y S1+S2 del departamento y
   la superficie sembrada media 2019/20–2024/25 de cada cultivo. Unión por nombre (los códigos de Chaco y Formosa del
   Hexoma no son los del MAGyP).
Aptitud no es uso (mercado, tenencia, rutas, agua de riego): se espera lift > 1 y correlación positiva, no coincidencia.

Salida: aptitud.validacion y pipeline/hexoma/validacion_aptitud.md.
Uso (desde neahub/):
    python pipeline/hexoma/validar_aptitud.py
"""
from __future__ import annotations

import difflib
import io
import os
import sys
import time
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from aptitud_reglas import NOMBRES  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
MAGYP = FUENTES_DIR / 'magyp' / 'estimaciones-agricolas-2026-03.csv'
INFORME = Path(__file__).resolve().parent / 'validacion_aptitud.md'
OBS = {'soja': ('cultivo', None), 'maiz': ('cultivo', None), 'algodon': ('cultivo', None), 'girasol': ('cultivo', None),
       'yerba': ('cultivo_perenne', ('misiones', 'corrientes')), 'te': ('cultivo_perenne', ('misiones', 'corrientes')),
       'citricos': ('cultivo_perenne', ('misiones', 'corrientes')), 'pasturas': ('pastura', None)}
MAGYP_CULT = {'soja': ['soja 1ra'], 'maiz': ['maíz'], 'algodon': ['algodón'], 'girasol': ['girasol'], 'arroz': ['arroz'],
              'yerba': ['yerba mate'], 'te': ['té'], 'citricos': ['naranja', 'mandarina', 'limón', 'pomelo']}


def norm(x):
    x = unicodedata.normalize('NFKD', str(x)).encode('ascii', 'ignore').decode().lower()
    for a, b in (('gral.', 'general'), ('gral ', 'general '), ('pte.', 'presidencia'), ('1° ', 'primero '),
                 ('1 de', 'primero de'), ('lib.', 'libertador'), ('cnel.', 'coronel'), ('dr.', 'doctor'), ('.', ' ')):
        x = x.replace(a, b)
    return ' '.join(x.split())


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    cols = ', '.join(f'r.{c}_clase' for c in NOMBRES)
    d = pd.read_sql(f"""SELECT r.h3::text AS h3, t.territorio, x.redcode, h3_cell_area(r.h3, 'km^2') AS km2, {cols},
                               u.cultivo, u.cultivo_perenne, u.pastura
                        FROM aptitud.resumen r
                        JOIN (SELECT DISTINCT ON (h3) h3, territorio_id, redcode FROM hexagonos) x USING (h3)
                        JOIN territorios t USING (territorio_id)
                        LEFT JOIN uso.h3_anio u ON u.h3 = r.h3 AND u.anio = 2025""", conn)
    filas = []
    for cult, (col, terr) in OBS.items():
        s = d if terr is None else d[d.territorio.isin(terr)]
        s = s[s[f'{cult}_clase'].notna()]
        obs = s[col] >= 500
        for k, rot in ((1, 'S1'), (2, 'S1+S2')):
            base = (s[f'{cult}_clase'] <= k).mean()
            en_obs = (s.loc[obs, f'{cult}_clase'] <= k).mean()
            filas.append(dict(cultivo=cult, prueba=f'lift MapBiomas 2025 ({col} ≥ 50 %)', clase=rot,
                              n=int(obs.sum()), valor_obs=100 * en_obs, valor_base=100 * base,
                              indicador=en_obs / base if base > 0 else None))
    # departamentos contra el MAGyP
    m = pd.read_csv(MAGYP, encoding='utf-8')
    m = m[m.provincia_id.isin([18, 22, 34, 54])]
    lim = pd.read_sql('SELECT l.redcode, l.nombre, t.etiqueta AS provincia FROM limites l JOIN territorios t USING (territorio_id)', conn)
    mapa = {}
    for prov, g in m.groupby('provincia'):
        nombres = {norm(n): r for n, r in zip(lim[lim.provincia == prov].nombre, lim[lim.provincia == prov].redcode)}
        for dep in g.departamento.unique():
            k = norm(dep)
            cerca = difflib.get_close_matches(k, list(nombres), 1, 0.75)
            if k in nombres or cerca:
                mapa[(prov, dep)] = nombres.get(k) or nombres[cerca[0]]
    m['redcode'] = [mapa.get((p_, d_)) for p_, d_ in zip(m.provincia, m.departamento)]
    m = m[m.redcode.notna()]
    for cult, nombres_m in MAGYP_CULT.items():
        mc = m[m.cultivo.isin(nombres_m)]
        # últimas 6 campañas con dato (2019/20–2024/25 para casi todos; los cítricos del MAGyP terminan en 1996/97)
        ult = sorted(mc.campania.unique())[-6:]
        mc = mc[mc.campania.isin(ult)]
        sem = mc.groupby(['redcode', 'campania']).superficie_sembrada_ha.sum().groupby('redcode').mean()
        if sem.sum() == 0:
            continue
        for k, rot in ((1, 'S1'), (2, 'S1+S2')):
            apt = d[d[f'{cult}_clase'] <= k].groupby('redcode').km2.sum() * 100
            j = pd.concat([apt.rename('apt'), sem.rename('sembrada')], axis=1).fillna(0)
            j = j[j.index.isin(d.redcode.unique())]
            if len(j) < 5:
                continue
            filas.append(dict(cultivo=cult, prueba=f'departamentos frente a superficie sembrada MAGyP {ult[0]}–{ult[-1]}',
                              clase=rot, n=len(j), valor_obs=np.corrcoef(j.apt, j.sembrada)[0, 1],
                              valor_base=j.apt.rank().corr(j.sembrada.rank()), indicador=None))
    v = pd.DataFrame(filas)
    with conn, conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS aptitud.validacion')
        cur.execute("""CREATE TABLE aptitud.validacion (cultivo text, prueba text, clase text, n int, valor_obs float8,
                       valor_base float8, indicador float8)""")
        buf = io.StringIO()
        v.to_csv(buf, index=False, header=False, na_rep='')
        buf.seek(0)
        cur.copy_expert("COPY aptitud.validacion FROM STDIN WITH (FORMAT csv, NULL '')", buf)
        cur.execute("COMMENT ON TABLE aptitud.validacion IS 'Lift: valor_obs = % de celdas cultivadas en la clase, valor_base = "
                    "% de todas las celdas, indicador = cociente. Departamentos: valor_obs = r de Pearson, valor_base = ρ de Spearman'")
    f = lambda x, nd=1: '' if pd.isna(x) else f'{x:,.{nd}f}'.replace(',', ' ').replace('.', ',')
    L = ['# Validación de la aptitud por cultivo', '',
         f'Generado por `pipeline/hexoma/validar_aptitud.py` el {time.strftime("%d-%m-%Y")}. Aptitud no es uso: se espera '
         'lift > 1 y correlación positiva, no coincidencia. Clase = clase mediana 1991/92–2024/25 (aptitud.resumen).', '',
         '## Lift contra MapBiomas 2025', '',
         '| Cultivo | Celdas cultivadas | Clase | % de cultivadas en la clase | % de todas | Lift |', '|---|---:|---|---:|---:|---:|']
    for r in v[v.prueba.str.startswith('lift')].itertuples():
        L.append(f'| {NOMBRES[r.cultivo]} | {r.n:,} | {r.clase} | {f(r.valor_obs)} | {f(r.valor_base)} | {f(r.indicador, 2)} |')
    L += ['', 'Yerba, té y cítricos se comparan con el «cultivo perenne» de MapBiomas, que en Misiones y Corrientes es sobre '
          'todo yerba y té: para cítricos el lift no es una prueba válida; vale la de departamentos.',
          '', '## Departamentos contra el MAGyP', '', '| Cultivo | Campañas MAGyP | Deptos. | Clase | r | ρ |', '|---|---|---:|---|---:|---:|']
    for r in v[v.prueba.str.startswith('dep')].itertuples():
        L.append(f'| {NOMBRES[r.cultivo]} | {r.prueba.split("MAGyP ")[1]} | {r.n} | {r.clase} | {f(r.valor_obs, 2)} | {f(r.valor_base, 2)} |')
    INFORME.write_text('\n'.join(L) + '\n', encoding='utf-8')
    print('\n'.join(L))
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
