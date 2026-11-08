"""Piloto: ¿un clasificador con Claude (CLI, suscripción) acierta el tipo que anoté a mano en las muestras v3?"""
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

D = Path(r'C:\Users\ant\OneDrive\nealab\neahub\pipeline\noticias')
OUT = D / 'piloto_clasificador_llm_haiku.csv'
MODELO = sys.argv[1] if len(sys.argv) > 1 else 'haiku'

INSTR = """Sos anotador de un archivo de prensa de Misiones (Argentina). Para cada oración de una nota periodística, con el
tipo de evento que le asignaron unas reglas, decidí:

- hecho: true si la oración informa un hecho de ese tipo que ocurrió o está ocurriendo (un corte de puente, un corte de
  ruta o camino, una crecida o desborde, personas evacuadas o aisladas). false si es un pronóstico, una advertencia, un
  condicional, una negación («no hubo cortes», «sin evacuados»), una opinión o un tema general («los cortes de ruta
  perjudican»), una obra para evitar inundaciones, un problema crónico sin hecho puntual, o si el tipo no corresponde
  (un corte de luz no es corte de ruta; «lluvias aisladas» no son personas aisladas). Un levantamiento o una
  rehabilitación («se habilitó el puente tras la crecida», «docentes levantaron el corte») cuenta como hecho: el corte
  existió.
- fuera: true si el hecho ocurre claramente fuera de la provincia de Misiones (otra provincia argentina, Paraguay, Brasil
  u otro país); false si es en Misiones o no se puede saber. Los puentes y pasos de frontera de Misiones cuentan como
  Misiones.
- motivo (sólo para cortes de puente o de ruta): protesta | accidente | agua | obra | otro (otro = no se lee en la oración).

Respondé SOLO con un arreglo JSON, un objeto por oración, en el mismo orden: {"i": <número>, "hecho": bool, "fuera": bool, "motivo": str|null}.
"""


def lote(filas):
    txt = INSTR + '\n\nOraciones:\n' + '\n'.join(
        f'{r.i}. [tipo: {r.tipo}; medio: {r.medio}; fecha: {r.fecha_nota}] {str(r.oracion)[:700]}' for r in filas.itertuples())
    p = subprocess.run(['claude', '-p', '--model', MODELO, '--output-format', 'json', '--allowedTools', ''],
                       input=txt, capture_output=True, text=True, encoding='utf-8', timeout=900)
    res = json.loads(p.stdout)['result']
    res = res[res.find('['):res.rfind(']') + 1]
    return json.loads(res)


def main():
    m = []
    for s in (12, 13, 14, 15):
        d = pd.read_csv(D / f'muestra_precision_v3_semilla{s}.csv').assign(semilla=s)
        m.append(d)
    m = pd.concat(m, ignore_index=True).reset_index(names='i')
    lotes = [m.iloc[k:k + 60] for k in range(0, len(m), 60)]
    with ThreadPoolExecutor(4) as ex:
        out = [x for r in ex.map(lote, lotes) for x in r]
    o = pd.DataFrame(out).rename(columns={'motivo': 'motivo_llm', 'hecho': 'hecho_llm', 'fuera': 'fuera_llm'})
    m = m.merge(o, on='i', how='left')
    m.to_csv(OUT, index=False, encoding='utf-8')
    t = m.tipo_ok.astype(int)
    h = m.hecho_llm.astype(bool)
    print(f'modelo {MODELO}; n {len(m)}; reglas solas: tipo {t.mean():.0%}')
    print(f'acuerdo hecho_llm vs tipo_ok: {(h == t.astype(bool)).mean():.0%}')
    print(f'precisión si se filtra con hecho_llm: {t[h].mean():.0%} (quedan {h.sum()}/{len(m)}); '
          f'se pierden {((~h) & (t == 1)).sum()} hechos reales de {t.sum()}')
    f = m.fuera_misiones.fillna(0).astype(int)
    keep = h & ~m.fuera_llm.astype(bool)
    print(f'fuera de Misiones: reglas {f.mean():.1%}; tras filtrar hecho y fuera del llm {f[keep].mean():.1%}')
    print(f'precisión tipo tras ambos filtros: {t[keep].mean():.0%} (quedan {keep.sum()})')
    for s, g in m.groupby('semilla'):
        k = g.hecho_llm.astype(bool) & ~g.fuera_llm.astype(bool)
        print(f'  semilla {s}: {g.tipo_ok.astype(int)[k].mean():.0%} de {k.sum()}')


if __name__ == '__main__':
    main()
