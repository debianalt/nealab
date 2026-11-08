"""Precisión por municipio-día antes y después del juicio de Haiku, sobre la muestra anotada a mano (8-nov-2026).

Lee muestra_celdas_v3_semilla<N>.csv (60 celdas de la matriz por agua, anotadas a mano: celda_ok), juzga con clasificar_llm.py
cada oración de cada celda y da la celda por buena si alguna oración la sostiene (clasificar_llm.vale). Compara con
la anotación.

Uso (desde neahub/):  python pipeline/noticias/celdas_llm.py <semilla>
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from build_hexoma import DB, connect  # noqa: E402
from clasificar_llm import MODELO, VERSION, clasificar, vale  # noqa: E402



def main() -> int:
    semilla = int(sys.argv[1]) if len(sys.argv) > 1 else 16
    conn = connect(DB)
    c = pd.read_csv(ROOT / 'pipeline' / 'noticias' / f'muestra_celdas_v3_semilla{semilla}.csv')
    c['dia'] = pd.to_datetime(c.dia)
    m = pd.read_sql("""SELECT v.municipio_gid, v.municipio, v.dia, mm.mencion_id, mm.oracion, mm.tipo, mm.medio,
                              mm.fecha_nota::date fecha_nota, n.titulo
                       FROM noticias.v_matriz_municipio_dia v, unnest(v.eventos_id) x
                       JOIN noticias.evento_fuentes f ON f.evento_id = x
                       JOIN noticias.menciones mm USING (mencion_id)
                       JOIN noticias.notas n ON n.nota_id = mm.nota_id
                       WHERE v.motivo = 'agua' AND v.dia >= '2012-01-01'""", conn)
    m['dia'] = pd.to_datetime(m.dia)
    m = m.merge(c[['municipio_gid', 'dia']], on=['municipio_gid', 'dia']).drop_duplicates(['municipio_gid', 'dia', 'mencion_id'])
    j = clasificar(conn, m)
    j['ok'] = vale(j)
    k = j.groupby(['municipio_gid', 'dia']).ok.any().rename('llm_ok').reset_index()
    c = c.merge(k, on=['municipio_gid', 'dia'], how='left')
    c['llm_ok'] = c.llm_ok.fillna(False).astype(bool)
    keep = c[c.llm_ok]
    print(f'semilla {semilla}; {MODELO} {VERSION}; celdas {len(c)}; oraciones juzgadas {len(j)}')
    print(f'precisión por celda: reglas {c.celda_ok.mean():.0%}; con Haiku {keep.celda_ok.mean():.0%} '
          f'(quedan {len(keep)}; se pierden {int(((~c.llm_ok) & (c.celda_ok == 1)).sum())} celdas buenas de '
          f'{int(c.celda_ok.sum())})')
    print('errores que quedan:')
    print(keep[keep.celda_ok == 0][['municipio', 'dia', 'nota']].to_string(index=False))
    print('buenas perdidas:')
    print(c[(~c.llm_ok) & (c.celda_ok == 1)][['municipio', 'dia']].to_string(index=False))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
