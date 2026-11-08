"""Validación del archivo de noticias → pipeline/noticias/validacion_noticias.md.

Regla fijada ANTES de mirar (plan del 7-nov-2026): precisión del TIPO de evento ≥ 80 % en una muestra al azar de 60
menciones (semilla 7); si no, se ajustan los patrones de extraer.py antes de usar el archivo.

Dos pasos:
    python pipeline/noticias/validar_noticias.py --muestra   # escribe pipeline/noticias/muestra_precision.csv (sin anotar)
    (se anotan a mano las columnas tipo_ok y lugar_ok: 1 / 0; lugar_ok vacío si la mención no tiene lugar)
    python pipeline/noticias/validar_noticias.py             # informe
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect  # noqa: E402
from validar_cruces import tabla  # noqa: E402

MD = ROOT / 'pipeline' / 'noticias' / 'validacion_noticias.md'
MUESTRA = ROOT / 'pipeline' / 'noticias' / 'muestra_precision.csv'
NUEVOS = ROOT / 'pipeline' / 'noticias' / 'muestra_precision_nuevos.csv'
PRENSA = ROOT / 'pipeline' / 'infra' / 'cortes_rp2_prensa.csv'
N, SEMILLA, REGLA = 60, 7, 0.80
DOMINIOS = {'misionesonline.net': 'misionesonline', 'primeraedicion.com.ar': 'primeraedicion',
            'elterritorio.com.ar': 'elterritorio', 'lavozdecataratas.com': 'lavozdecataratas',
            'misionescuatro.com': 'misionescuatro',
            'canal12misiones.com': 'canal12', 'noticiasdel6.com': 'noticiasdel6', 'economis.com.ar': 'economis',
            'misionesopina.com.ar': 'misionesopina'}


def url_norm(u: str) -> str:
    u = str(u).strip().lower().replace('http://', 'https://').replace('://www.', '://')
    return u.split('?')[0].split('#')[0].rstrip('/')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--muestra', action='store_true')
    ap.add_argument('--semilla', type=int, default=SEMILLA)
    ap.add_argument('--medios', default='', help='sólo estos medios (coma); la muestra va a muestra_precision_nuevos.csv')
    ap.add_argument('--n', type=int, default=N)
    a = ap.parse_args()
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    if a.muestra:
        m = q("""SELECT m.mencion_id, m.medio, m.fecha::date fecha, m.tipo, m.lugar_tipo, m.lugar_nombre, m.oracion
                 FROM noticias.menciones m""")
        destino = MUESTRA
        if a.medios:
            m = m[m.medio.isin(a.medios.split(','))]
            destino = NUEVOS
        s = m.sample(a.n, random_state=a.semilla).sort_values('mencion_id')
        s['tipo_ok'] = ''
        s['lugar_ok'] = ''
        s['nota'] = ''
        s.to_csv(destino, index=False, encoding='utf-8')
        print(f'muestra: {destino} ({len(s)} menciones, semilla {a.semilla})')
        return 0

    L = ['# Validación del archivo de noticias (cortes de puentes y caminos, crecidas, evacuados)', '',
         f'Generado por `pipeline/noticias/validar_noticias.py` el {dt.date.today().isoformat()}. Datos: 6 medios con '
         'API de WordPress abierta, El Territorio y La Voz de Cataratas por sitemaps y HTML, MisionesCuatro por copias '
         'de la Wayback Machine (su sitio desafía a todo cliente que no sea un navegador) y el archivo completo de '
         'Misiones Online por sus sitemaps; extracción por reglas (`extraer.py`).', '']
    L += ['## 1. Notas por medio', '', tabla(q("""
        SELECT medio, coalesce(via, 'api_wp') via, count(*)::int notas, min(fecha)::date::text desde,
               max(fecha)::date::text hasta FROM noticias.notas GROUP BY 1, 2 ORDER BY 3 DESC""")), '',
          'Notas por año (todos los medios):', '', tabla(q("""
        SELECT extract(year FROM fecha)::int::text anio, count(*)::int notas FROM noticias.notas GROUP BY 1 ORDER BY 1""")), '']

    pr = pd.read_csv(PRENSA)
    pr['dominio'] = pr.url.str.extract(r'https?://(?:www\.)?([^/]+)')[0]
    pr['medio'] = pr.dominio.map(DOMINIOS)
    corpus = q('SELECT medio, wp_id, url FROM noticias.notas')
    urls = set(corpus.url.map(url_norm))
    ids = set(zip(corpus.medio, corpus.wp_id.astype(int)))
    cub = pr[pr.medio.notna()].copy()
    # enlaces cortos de WordPress (?p=ID): se comparan por medio y número de nota, no por dirección
    cub['wp_id'] = pd.to_numeric(cub.url.str.extract(r'[?&]p=(\d+)')[0], errors='coerce')
    cub['en_corpus'] = cub.url.map(url_norm).isin(urls) | [
        (m, int(i)) in ids if pd.notna(i) else False for m, i in zip(cub.medio, cub.wp_id)]
    por = cub.drop_duplicates('url').groupby('medio').agg(notas_del_agente=('url', 'size'), en_corpus=('en_corpus', 'sum'))
    L += ['## 2. Cobertura sobre las notas del agente de la fase 3', '',
          f'De {pr.url.nunique()} notas distintas de `cortes_rp2_prensa.csv`, {cub.url.nunique()} son de los 6 medios con API; '
          f'{int(cub.drop_duplicates("url").en_corpus.sum())} están en el corpus.', '', tabla(por.reset_index()), '',
          'Notas del agente de esos medios que no están (para revisar consultas):', '',
          tabla(cub[~cub.en_corpus].drop_duplicates('url')[['fecha', 'medio', 'lugar_textual', 'url']]), '']

    L += ['## 3. Menciones', '', tabla(q("""
        SELECT tipo, count(*)::int menciones, count(DISTINCT nota_id)::int notas,
               round(100.0 * count(*) FILTER (WHERE lugar_tipo IS NOT NULL) / count(*), 1)::float "% con lugar",
               round(100.0 * count(*) FILTER (WHERE ambiguo) / nullif(count(*) FILTER (WHERE lugar_tipo IS NOT NULL), 0), 1)::float "% lugar ambiguo"
        FROM noticias.menciones GROUP BY 1 ORDER BY 2 DESC""")), '',
          'Lugares más mencionados en cortes de puente o camino:', '', tabla(q("""
        SELECT lugar_tipo, lugar_nombre, count(*)::int menciones, count(DISTINCT nota_id)::int notas
        FROM noticias.menciones WHERE tipo IN ('corte_puente', 'corte_ruta_camino') AND lugar_nombre IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 25""")), '']

    v1 = MUESTRA.with_name('muestra_precision_v1.csv')
    if v1.exists():
        s1 = pd.read_csv(v1)
        L += ['## 4a. Primera medición (patrones v1, semilla 7)', '',
              f'Precisión del tipo {s1.tipo_ok.mean():.0%} (NO PASA la regla de {REGLA:.0%}); lugar '
              f'{s1.lugar_ok.dropna().mean():.0%}. Errores típicos: «aislados» en fútbol/virus, notas de Energía de Misiones '
              'sin corte, cortes de ruta de protesta, «habilitado» de obras, desbordes en el fútbol, frases hipotéticas. '
              'Se ajustaron los patrones (v2, comentario en extraer.py) y se midió con una muestra NUEVA (semilla 8), no '
              'con la misma.', '', tabla(s1.groupby('tipo').agg(n=('tipo_ok', 'size'), tipo_ok=('tipo_ok', 'mean'))
                                         .round(2).reset_index()), '']
    if MUESTRA.exists():
        s = pd.read_csv(MUESTRA)
        s = s[s.tipo_ok.notna()]
        if len(s):
            pt = s.tipo_ok.astype(int).mean()
            sl = s[s.lugar_ok.notna()]
            pl = sl.lugar_ok.astype(int).mean() if len(sl) else float('nan')
            L += ['## 4b. Precisión de las reglas vigentes (patrones v2, muestra al azar NUEVA, semilla 8)', '',
                  f'Regla fijada antes de mirar: precisión del tipo ≥ {REGLA:.0%}. Anotadas {len(s)} de {N}.', '',
                  f'**Tipo: {pt:.0%} ({"PASA" if pt >= REGLA else "NO PASA"}).** Lugar (menciones con lugar, {len(sl)}): '
                  f'{pl:.0%}.', '', 'Por tipo:', '',
                  tabla(s.groupby('tipo').agg(n=('tipo_ok', 'size'), tipo_ok=('tipo_ok', 'mean')).round(2).reset_index()), '',
                  'Errores (para ajustar patrones):', '',
                  tabla(s[(s.tipo_ok.astype(int) == 0) | (s.lugar_ok.fillna(1).astype(int) == 0)][
                      ['mencion_id', 'tipo', 'lugar_nombre', 'tipo_ok', 'lugar_ok', 'nota']]), '']
    if NUEVOS.exists():
        sn = pd.read_csv(NUEVOS)
        sn = sn[sn.tipo_ok.notna()]
        if len(sn):
            pt = sn.tipo_ok.astype(int).mean()
            sl = sn[sn.lugar_ok.notna()]
            L += ['## 4c. Precisión en los medios nuevos (El Territorio, La Voz, MisionesCuatro; semilla 9)', '',
                  f'Regla fijada antes de mirar: tipo ≥ {REGLA:.0%} (el HTML puede colar menús y títulos relacionados). '
                  f'Anotadas {len(sn)}.', '',
                  f'**Tipo: {pt:.0%} ({"PASA" if pt >= REGLA else "NO PASA"}).** Lugar ({len(sl)} con lugar): '
                  f'{(sl.lugar_ok.astype(int).mean() if len(sl) else float("nan")):.0%}.', '',
                  tabla(sn[(sn.tipo_ok.astype(int) == 0) | (sn.lugar_ok.fillna(1).astype(int) == 0)][
                      ['mencion_id', 'medio', 'tipo', 'lugar_nombre', 'tipo_ok', 'lugar_ok', 'nota']]), '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
