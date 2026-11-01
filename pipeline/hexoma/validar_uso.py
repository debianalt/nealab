"""Hexoma A2 — valida MapBiomas Col. 3 por celda contra las estimaciones agrícolas del MAGyP por departamento.

Dato independiente: MAGyP, Estimaciones agrícolas (datos.magyp.gob.ar, archivo 2026-03), superficie sembrada por
departamento y campaña. Comparaciones por departamento del NEA argentino (departamento de la celda = el de su
centro, hexagonos.redcode = departamento_id del MAGyP):
  - cultivo temporal (clase 19) del mapa del año Y frente a los cultivos de verano de la campaña (Y−1)/Y y Y/(Y+1):
    algodón + arroz + girasol + maíz + soja de primera + sorgo (sin soja de segunda: repite la superficie del trigo);
  - cultivo perenne (clase 36) frente a yerba mate + té + naranja + mandarina + limón + pomelo.
Métricas: correlación de Pearson y de Spearman entre departamentos, cociente de totales (MapBiomas / MAGyP).
La superficie sembrada no es superficie de cobertura: el cociente no tiene por qué ser 1; la correlación dice si
el mapa ubica bien la agricultura.

Salida: uso.validacion y pipeline/hexoma/validacion_uso.md.
Uso (desde neahub/):
    python pipeline/hexoma/validar_uso.py
"""
from __future__ import annotations

import io
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
MAGYP_URL = ('https://datos.magyp.gob.ar/dataset/9e1e77ba-267e-4eaa-a59f-3296e86b5f36/resource/'
             '95d066e6-8a0f-4a80-b59d-6f28f88eacd5/download/estimaciones-agricolas-2026-03.csv')
MAGYP = FUENTES_DIR / 'magyp' / 'estimaciones-agricolas-2026-03.csv'
INFORME = Path(__file__).resolve().parent / 'validacion_uso.md'
VERANO = ['algodón', 'arroz', 'girasol', 'maíz', 'soja 1ra', 'sorgo']
PERENNES = ['yerba mate', 'té', 'naranja', 'mandarina', 'limón', 'pomelo']
ANIOS = [2000, 2005, 2010, 2015, 2020, 2025]


def main() -> None:
    t0 = time.time()
    if not MAGYP.exists():
        import subprocess
        MAGYP.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(['curl', '-s', '-f', '-o', str(MAGYP), MAGYP_URL], check=True)
    m = pd.read_csv(MAGYP, encoding='utf-8')
    m = m[m.provincia_id.isin([18, 22, 34, 54])].copy()
    m['dep'] = m.departamento_id.astype('Int64').astype(str).str.zfill(5)
    m['inicio'] = m.campania.str[:4].astype(int)
    conn = connect(DB)
    uso = pd.read_sql("""SELECT x.redcode AS dep, u.anio,
                                sum(u.cultivo * h3_cell_area(u.h3, 'km^2')) / 10 AS temporal_ha,
                                sum(u.cultivo_perenne * h3_cell_area(u.h3, 'km^2')) / 10 AS perenne_ha
                         FROM uso.h3_anio u JOIN (SELECT DISTINCT ON (h3) h3, redcode FROM hexagonos) x USING (h3)
                         WHERE u.anio = ANY(%(a)s) AND x.redcode IS NOT NULL GROUP BY 1, 2""", conn, params={'a': ANIOS})
    filas = []
    for a in ANIOS:
        u = uso[uso.anio == a].set_index('dep')
        for desfase, rot in ((-1, f'{a - 1}/{str(a)[2:]}'), (0, f'{a}/{str(a + 1)[2:]}')):
            c = m[(m.inicio == a + desfase)]
            for clase, cultivos, col in (('cultivo temporal', VERANO, 'temporal_ha'), ('cultivo perenne', PERENNES, 'perenne_ha')):
                s = c[c.cultivo.isin(cultivos)].groupby('dep').superficie_sembrada_ha.sum()
                if s.sum() == 0:
                    continue
                j = pd.concat([u[col], s.rename('magyp')], axis=1).fillna(0)
                j = j[(j[col] > 0) | (j.magyp > 0)]
                if len(j) < 5:
                    continue
                filas.append(dict(clase=clase, anio_mapa=a, campania=rot, departamentos=len(j),
                                  mapbiomas_ha=j[col].sum(), magyp_ha=j.magyp.sum(),
                                  cociente=j[col].sum() / j.magyp.sum(),
                                  r=np.corrcoef(j[col], j.magyp)[0, 1], rho=j[col].rank().corr(j.magyp.rank())))
    v = pd.DataFrame(filas)
    with conn, conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS uso.validacion')
        cur.execute("""CREATE TABLE uso.validacion (clase text, anio_mapa smallint, campania text, departamentos int,
                       mapbiomas_ha float8, magyp_ha float8, cociente float8, r float8, rho float8)""")
        buf = io.StringIO()
        v.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert('COPY uso.validacion FROM STDIN WITH (FORMAT csv)', buf)
        cur.execute("COMMENT ON TABLE uso.validacion IS 'MapBiomas Col. 3 por departamento frente a la superficie "
                    "sembrada del MAGyP (Estimaciones agrícolas, 2026-03). pipeline/hexoma/validar_uso.py'")
    f = lambda x, nd=0: f'{x:,.{nd}f}'.replace(',', ' ').replace('.', ',')
    L = ['# Validación del uso del suelo (MapBiomas Col. 3) contra el MAGyP', '',
         f'Generado por `pipeline/hexoma/validar_uso.py` el {time.strftime("%d-%m-%Y")}. Superficie por departamento '
         'del NEA argentino: MapBiomas (suma de ‰ × área de las celdas) frente a la superficie sembrada del MAGyP. '
         'Cultivos de verano = algodón, arroz, girasol, maíz, soja de primera y sorgo; perennes = yerba, té y cítricos. '
         'La superficie sembrada no es cobertura: importa más la correlación entre departamentos que el cociente.', '',
         '| Clase | Año del mapa | Campaña MAGyP | Deptos. | MapBiomas (ha) | MAGyP (ha) | Cociente | r | ρ |',
         '|---|---:|---|---:|---:|---:|---:|---:|---:|']
    for r in v.itertuples():
        L.append(f'| {r.clase} | {r.anio_mapa} | {r.campania} | {r.departamentos} | {f(r.mapbiomas_ha)} | '
                 f'{f(r.magyp_ha)} | {f(r.cociente, 2)} | {f(r.r, 2)} | {f(r.rho, 2)} |')
    INFORME.write_text('\n'.join(L) + '\n', encoding='utf-8')
    print(v.round(2).to_string(index=False))
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
