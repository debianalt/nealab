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
# v3 (7-nov-2026): reglas fijadas antes de mirar: tipo ≥ 85 %, motivo ≥ 80 %, lugar ≥ 85 %, fuera de Misiones ≤ 5 %;
# punto a ≤ 2 km del lugar real en ≥ 85 % de 40 ubicaciones finas
V3 = ROOT / 'pipeline' / 'noticias' / 'muestra_precision_v3.csv'
V3_UBIC = ROOT / 'pipeline' / 'noticias' / 'muestra_ubicacion_v3.csv'
SEMILLA_V3 = 12
REGLAS_V3 = {'tipo_ok': 0.85, 'motivo_ok': 0.80, 'lugar_ok': 0.85}
FINOS = ['cruce', 'cruce_inferido', 'puente', 'km', 'paraje', 'barrio', 'picada', 'escuela', 'nominatim', 'manual']
DOMINIOS = {'misionesonline.net': 'misionesonline', 'primeraedicion.com.ar': 'primeraedicion',
            'elterritorio.com.ar': 'elterritorio', 'lavozdecataratas.com': 'lavozdecataratas',
            'misionescuatro.com': 'misionescuatro',
            'canal12misiones.com': 'canal12', 'noticiasdel6.com': 'noticiasdel6', 'economis.com.ar': 'economis',
            'misionesopina.com.ar': 'misionesopina', 'misiones.gob.ar': 'gobmisiones'}


def url_norm(u: str) -> str:
    u = str(u).strip().lower().replace('http://', 'https://').replace('://www.', '://')
    return u.split('?')[0].split('#')[0].rstrip('/')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--muestra', action='store_true')
    ap.add_argument('--semilla', type=int, default=SEMILLA)
    ap.add_argument('--medios', default='', help='sólo estos medios (coma); la muestra va a muestra_precision_nuevos.csv')
    ap.add_argument('--n', type=int, default=N)
    ap.add_argument('--muestra-v3', action='store_true', help='muestras de la v3: 60 menciones y 40 ubicaciones finas')
    a = ap.parse_args()
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    if a.muestra_v3:
        m = q("""SELECT mencion_id, medio, fecha_nota::date fecha_nota, fecha_evento, fecha_evento_fuente, tipo, motivo,
                        precision_lugar, lugar_nombre, contexto, oracion FROM noticias.menciones""")
        s = m.sample(N, random_state=SEMILLA_V3).sort_values('mencion_id')
        s = s.assign(tipo_ok='', motivo_ok='', lugar_ok='', fecha_ok='', fuera_misiones='', nota='')
        s.to_csv(V3, index=False, encoding='utf-8')
        f = q(f"""SELECT mencion_id, tipo, precision_lugar, lugar_nombre, fuente_ubicacion, round(ST_X(geom)::numeric, 5) lon,
                         round(ST_Y(geom)::numeric, 5) lat, oracion FROM noticias.menciones
                  WHERE precision_lugar IN ({', '.join(repr(x) for x in FINOS)})""")
        f = f.sample(min(40, len(f)), random_state=SEMILLA_V3).sort_values('mencion_id').assign(punto_ok='', nota='')
        f.to_csv(V3_UBIC, index=False, encoding='utf-8')
        print(f'muestras v3: {V3} ({len(s)}), {V3_UBIC} ({len(f)}), semilla {SEMILLA_V3}')
        return 0
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

    L += ['## 3. Menciones (patrones v3)', '', tabla(q("""
        SELECT tipo, motivo, count(*)::int menciones, count(DISTINCT nota_id)::int notas,
               round(100.0 * count(*) FILTER (WHERE geom IS NOT NULL) / count(*), 1)::float "% con lugar"
        FROM noticias.menciones GROUP BY 1, 2 ORDER BY 3 DESC""")), '',
          'Precisión de la ubicación:', '', tabla(q("""
        SELECT coalesce(precision_lugar, 'sin lugar') AS precision_lugar, count(*)::int menciones
        FROM noticias.menciones GROUP BY 1 ORDER BY 2 DESC""")), '',
          'Fecha del evento:', '', tabla(q("""
        SELECT fecha_evento_fuente, count(*)::int menciones FROM noticias.menciones GROUP BY 1 ORDER BY 2 DESC""")), '']
    if q("SELECT to_regclass('noticias.eventos') IS NOT NULL ok").ok[0]:
        L += ['## 3b. Eventos (`eventos.py`)', '', tabla(q("""
            SELECT tipo, motivo, count(*)::int eventos, count(*) FILTER (WHERE n_medios >= 2)::int "2+ medios",
                   count(*) FILTER (WHERE n_oficiales > 0)::int "con fuente oficial"
            FROM noticias.eventos GROUP BY 1, 2 ORDER BY 3 DESC""")), '']

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
    if V3.exists():
        s3 = pd.read_csv(V3)
        s3 = s3[s3.tipo_ok.notna()]
        if len(s3):
            filas = []
            for c, regla in REGLAS_V3.items():
                x = s3[c].dropna().astype(int)
                filas.append(dict(medida=c, n=len(x), valor=round(x.mean(), 2), regla=regla,
                                  resultado='PASA' if x.mean() >= regla else 'NO PASA'))
            fu = s3.fuera_misiones.fillna(0).astype(int).mean()
            filas.append(dict(medida='fuera_misiones', n=len(s3), valor=round(fu, 2), regla='≤ 0.05',
                              resultado='PASA' if fu <= 0.05 else 'NO PASA'))
            fe = s3.fecha_ok.dropna().astype(int)
            filas.append(dict(medida='fecha_ok (sin regla)', n=len(fe), valor=round(fe.mean(), 2), regla='—', resultado='—'))
            L += [f'## 5. Precisión de la v3 (muestra al azar de {N}, semilla {SEMILLA_V3})', '',
                  'Reglas fijadas antes de mirar. lugar_ok: el punto corresponde al lugar del hecho a la escala de su '
                  'precisión (un contexto de nota cuenta como acierto si el hecho ocurrió en ese municipio).', '',
                  tabla(pd.DataFrame(filas)), '', 'Errores:', '',
                  tabla(s3[(s3[list(REGLAS_V3)].fillna(1).astype(int) == 0).any(axis=1) | (s3.fuera_misiones.fillna(0) == 1)][
                      ['mencion_id', 'tipo', 'motivo', 'precision_lugar', 'lugar_nombre', 'nota']]), '']
    if V3_UBIC.exists():
        u3 = pd.read_csv(V3_UBIC)
        u3 = u3[u3.punto_ok.notna()]
        if len(u3):
            pu = u3.punto_ok.astype(int).mean()
            L += [f'## 6. Ubicaciones finas (40 al azar, semilla {SEMILLA_V3})', '',
                  f'Regla: punto a ≤ 2 km del lugar real en ≥ 85 %. **{pu:.0%} ({"PASA" if pu >= 0.85 else "NO PASA"}).**', '',
                  tabla(u3.groupby('precision_lugar').agg(n=('punto_ok', 'size'), ok=('punto_ok', 'mean')).round(2).reset_index()),
                  '', 'Errores:', '', tabla(u3[u3.punto_ok.astype(int) == 0][['mencion_id', 'precision_lugar', 'lugar_nombre', 'nota']]), '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
