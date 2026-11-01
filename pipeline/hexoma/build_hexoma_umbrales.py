"""Hexoma A4 (paso 1) — carga la propuesta de umbrales por cultivo y su evidencia en el esquema `meta`.

    meta.umbrales_cultivo     la propuesta (umbrales_cultivos.md): cultivo × factor, S1, límite, fuente, nota y estado
                              ('propuesto' hasta que el autor la valide; la aptitud no se calcula antes)
    meta.umbrales_evidencia   todas las filas con cita de umbrales_evidencia_*.md (cultivo, variable, tipo, valores,
                              unidad, condición, fuente, página, URL, cita textual)

Idempotente. Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_umbrales.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

AQUI = Path(__file__).resolve().parent


def celdas(linea: str) -> list[str]:
    return [c.strip() for c in linea.strip().strip('|').split('|')]


def tablas(md: str):
    """(encabezado, filas, sección) de cada tabla markdown."""
    seccion, enc, filas = None, None, []
    for linea in md.splitlines() + ['']:
        if linea.startswith('#'):
            seccion = linea.lstrip('#').strip()
        if linea.startswith('|'):
            c = celdas(linea)
            if enc is None:
                enc = [x.lower() for x in c]
            elif set(''.join(c)) <= set('-: '):
                continue
            else:
                filas.append(c)
        elif enc is not None:
            yield enc, filas, seccion
            enc, filas = None, []


def main() -> None:
    propuesta = []
    for enc, filas, sec in tablas((AQUI / 'umbrales_cultivos.md').read_text(encoding='utf-8')):
        if enc[:3] == ['factor', 's1 (óptimo)', 'límite (n fuera)'] or enc[:2] == ['factor', 's1']:
            for f in filas:
                propuesta.append((sec, *f[:5]))
    evid = []
    for archivo in sorted(AQUI.glob('umbrales_evidencia_*.md')):
        grupo = archivo.stem.replace('umbrales_evidencia_', '')
        for enc, filas, _ in tablas(archivo.read_text(encoding='utf-8')):
            if 'cultivo' not in enc or 'variable' not in enc:
                continue
            col = {k: i for i, k in enumerate(enc)}

            def g(f, *nombres):
                for n in nombres:
                    if n in col and col[n] < len(f):
                        return f[col[n]]
                return None
            for f in filas:
                evid.append((grupo, g(f, 'cultivo'), g(f, 'variable'), g(f, 'tipo'), g(f, 'valor_min', 'min'),
                             g(f, 'valor_max', 'max'), g(f, 'unidad'), g(f, 'etapa o condición', 'condición'),
                             g(f, 'cita completa', 'fuente'), g(f, 'página/tabla', 'página'), g(f, 'url/doi'),
                             g(f, 'cita textual')))
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS meta.umbrales_cultivo, meta.umbrales_evidencia')
        cur.execute("""CREATE TABLE meta.umbrales_cultivo (cultivo text, factor text, s1 text, limite text, fuente text,
                       nota text, estado text DEFAULT 'validado 2026-11-01', PRIMARY KEY (cultivo, factor))""")
        for r in propuesta:
            cur.execute('INSERT INTO meta.umbrales_cultivo (cultivo, factor, s1, limite, fuente, nota) VALUES (%s, %s, %s, %s, %s, %s)',
                        tuple(r) + (None,) * (6 - len(r)))
        cur.execute("COMMENT ON TABLE meta.umbrales_cultivo IS 'A4: umbrales por cultivo validados por el autor el 1-nov-2026 "
                    "(umbrales_cultivos.md); reglas en meta.umbrales_reglas'")
        cur.execute('DROP TABLE IF EXISTS meta.umbrales_reglas')
        cur.execute('CREATE TABLE meta.umbrales_reglas (n smallint PRIMARY KEY, regla text)')
        md = (AQUI / 'umbrales_cultivos.md').read_text(encoding='utf-8')
        bloque = md.split('## Reglas decididas', 1)[1].split(chr(10) + '## ', 1)[0]
        reglas, actual = [], None
        for l in bloque.splitlines()[1:]:
            m = re.match(r'^(\d+)\.\s+(.*)', l)
            if m:
                actual = [int(m.group(1)), m.group(2)]
                reglas.append(actual)
            elif l.strip() and actual:
                actual[1] += ' ' + l.strip()
        cur.executemany('INSERT INTO meta.umbrales_reglas VALUES (%s, %s)', [tuple(r) for r in reglas])
        cur.execute("COMMENT ON TABLE meta.umbrales_reglas IS 'A4: reglas decididas por el autor el 1-nov-2026 (precedencia de "
                    "fuentes, pendiente, pH, calor, arroz, campo natural, combinación de factores)'")
        cur.execute("""CREATE TABLE meta.umbrales_evidencia (grupo text, cultivo text, variable text, tipo text, valor_min text,
                       valor_max text, unidad text, condicion text, fuente text, pagina text, url text, cita_textual text)""")
        cur.executemany('INSERT INTO meta.umbrales_evidencia VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', evid)
        cur.execute("COMMENT ON TABLE meta.umbrales_evidencia IS 'A4: cada umbral publicado con cita, página, URL y cita "
                    "textual (umbrales_evidencia_*.md). Las citas de heladas y lluvia se verificaron contra los textos bajados'")
    log(f'propuesta: {len(propuesta)} filas en {len({p[0] for p in propuesta})} cultivos; evidencia: {len(evid)} filas')


if __name__ == '__main__':
    main()
