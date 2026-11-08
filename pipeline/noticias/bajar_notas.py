"""Archivo de notas de prensa de Misiones sobre cortes de puentes y caminos, crecidas y evacuados (sólo datos).

Medios con la API de WordPress abierta (verificado el 7-nov-2026), más el portal del Gobierno (misiones.gob.ar,
fuente oficial): /wp-json/wp/v2/posts?search=<consulta>, paginado de a 100. Para cada medio y cada consulta de
CONSULTAS se bajan TODAS las páginas (texto completo). User-Agent honesto y una pausa de PAUSA_S entre pedidos al
mismo medio; los medios corren en paralelo. La carga SÓLO SUMA notas nuevas (nunca borra ni reescribe una archivada).

El Territorio, La Voz de Cataratas, MisionesCuatro y el archivo completo de Misiones Online: bajar_sin_api.py.

El archivo NO se regenera (las notas se borran o cambian): los crudos quedan en %HEXOMA_FUENTES%/noticias/crudo/ y una
copia comprimida en C:/Users/ant/OneDrive/nealab/archivo_noticias/ (OneDrive = respaldo).

  noticias.notas    medio, wp_id, fecha, url, titulo, texto (sin HTML), categorias, consultas, bajada
  noticias.fuentes  medio, endpoint, consultas, notas, primera y última fecha

Uso (desde neahub/):
    python pipeline/noticias/bajar_notas.py            # baja lo que falta (reanudable) y carga
    python pipeline/noticias/bajar_notas.py --solo-cargar
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import io
import json
import os
import re
import shutil
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from comun import actualizar_fuentes, limpiar_texto  # noqa: E402

DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'noticias'
RESPALDO = Path(r'C:\Users\ant\OneDrive\nealab\archivo_noticias')
UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar)'}
PAUSA_S = 1.0
MEDIOS = {
    'misionesonline': 'https://misionesonline.net',
    'primeraedicion': 'https://www.primeraedicion.com.ar',
    'canal12': 'https://www.canal12misiones.com',
    'noticiasdel6': 'https://noticiasdel6.com',
    'economis': 'https://economis.com.ar',
    'misionesopina': 'https://www.misionesopina.com.ar',
    # 7-nov-2026: fuente oficial. Vialidad Provincial y Defensa Civil no publican partes propios online; sus
    # gacetillas salen en el portal del Gobierno (WordPress, API abierta, desde oct-2018)
    'gobmisiones': 'https://misiones.gob.ar',
}
# La búsqueda de WordPress busca cada palabra ADENTRO de cualquier palabra (LIKE %x%) y las combina con Y: «vado»
# encuentra «privado» (16–29 mil notas por medio), «EMSA» encuentra «empresa», y «sin luz» sin comillas encuentra
# toda nota con «sin» y «luz» (3.076 contra 76 en Economis). Entre comillas busca la frase exacta: se usa sólo donde
# hay palabras comunes; «puente cortado» como frase deja afuera «el puente quedó cortado» (1 contra 31). (7-nov-2026)
CONSULTAS = [
    'puente cortado', 'puente sobre el arroyo', 'ruta cortada', 'ruta provincial cortada', 'intransitable',
    'camino vecinal', 'alcantarilla', 'desborde', 'crecida del arroyo', 'crecida del río', '"sin luz"',
    'corte de energía', '"sin energía"', '"cortes de luz"', '"Energía de Misiones"', 'temporal', 'evacuados',
    'aislados',
    # 7-nov-2026: el buscador de Misiones Online trabaja por palabra entera y no traía notas con «clausurado»,
    # «inhabilitada» o «interrumpirá» (5 de las 11 notas del agente de la fase 3)
    'crecida', 'clausurado', 'inhabilitado', 'inhabilitada', 'interrumpido', 'interrumpirá',
    # 7-nov-2026 (v3): cortes de toda clase, piquetes incluidos («expertos en infraestructura»)
    '"corte de ruta"', 'piquete', 'manifestantes', '"corte total"', '"corte parcial"', '"puente clausurado"',
    '"corte de tránsito"',
]


def slug(s: str) -> str:
    frase = s.startswith('"')
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return ('frase_' if frase else '') + re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


def bajar_medio(medio: str, base: str, desde: dt.date | None = None) -> dict:
    """desde: sólo notas publicadas después (parámetro after de WordPress); van a crudo/<medio>/inc_<desde>/"""
    s = requests.Session()
    s.headers.update(UA)
    resumen = {}
    for q in CONSULTAS:
        d = DIR / 'crudo' / medio / (f'inc_{desde:%Y%m%d}' if desde else '')
        d.mkdir(parents=True, exist_ok=True)
        pagina, total_pag, n = 1, None, 0
        while True:
            dest = d / f'{slug(q)}_p{pagina:03d}.json'
            if dest.exists():
                datos = json.loads(dest.read_text(encoding='utf-8'))
                meta = json.loads((d / f'{slug(q)}_meta.json').read_text(encoding='utf-8')) if (d / f'{slug(q)}_meta.json').exists() else {}
                total_pag = meta.get('paginas', total_pag)
            else:
                for intento in range(4):
                    try:
                        r = s.get(f'{base}/wp-json/wp/v2/posts', timeout=90, params={
                            'search': q, 'per_page': 100, 'page': pagina,
                            '_fields': 'id,date,link,title,content,excerpt,categories',
                            **({'after': f'{desde:%Y-%m-%d}T00:00:00'} if desde else {})})
                        break
                    except requests.RequestException:
                        time.sleep(10 * (intento + 1))
                else:
                    raise RuntimeError(f'{medio} «{q}» p{pagina}: sin respuesta')
                time.sleep(PAUSA_S)
                if r.status_code == 400 and 'rest_post_invalid_page_number' in r.text:
                    break
                r.raise_for_status()
                datos = r.json()
                total_pag = int(r.headers.get('X-WP-TotalPages', 1))
                (d / f'{slug(q)}_meta.json').write_text(json.dumps({
                    'consulta': q, 'total': int(r.headers.get('X-WP-Total', 0)), 'paginas': total_pag,
                    'bajado': dt.datetime.now().isoformat(timespec='seconds')}), encoding='utf-8')
                dest.write_text(json.dumps(datos, ensure_ascii=False), encoding='utf-8')
            n += len(datos)
            if not datos or (total_pag and pagina >= total_pag):
                break
            pagina += 1
        resumen[q] = n
        log(f'  {medio} «{q}»: {n:,} notas')
    return resumen


def texto_plano(h: str) -> str:
    h = re.sub(r'(?is)<(script|style|figure|iframe)[^>]*>.*?</\1>', ' ', h or '')
    h = re.sub(r'(?i)<br\s*/?>|</p>|</h[1-6]>|</li>', '\n', h)
    h = re.sub(r'<[^>]+>', ' ', h)
    h = html.unescape(h)
    h = re.sub(r'[ \t\xa0]+', ' ', h)
    return re.sub(r'\n\s*\n+', '\n', h).strip()


def cargar(respaldo: bool = True) -> int:
    filas = {}
    vigentes = {slug(q): q for q in CONSULTAS}   # crudos de consultas descartadas (p. ej. «vado») no entran
    for medio in MEDIOS:
        for p in sorted((DIR / 'crudo' / medio).rglob('*_p*.json')):   # incluye las bajadas incrementales inc_*/
            q = p.stem.rsplit('_p', 1)[0]
            if q not in vigentes:
                continue
            q = vigentes[q]
            for a in json.loads(p.read_text(encoding='utf-8')):
                k = (medio, int(a['id']))
                if k not in filas:
                    filas[k] = {'medio': medio, 'wp_id': int(a['id']), 'fecha': a['date'], 'url': a['link'],
                                'titulo': texto_plano(a['title']['rendered']),
                                'texto': texto_plano(a['content']['rendered']),
                                'categorias': json.dumps(a.get('categories', [])), 'consultas': set()}
                filas[k]['consultas'].add(q)
    df = pd.DataFrame(filas.values())
    df['consultas'] = df.consultas.map(lambda s: ';'.join(sorted(s)))
    df['texto'] = df.texto.map(limpiar_texto)
    df['bajada'] = dt.date.today().isoformat()
    conn = connect(DB)
    cur = conn.cursor()
    # SÓLO SUMA (7-nov-2026): antes era DROP + CREATE y habría borrado las notas de sitemaps y Wayback
    # (bajar_sin_api.py) y renumerado nota_id. Una nota ya archivada no se reescribe: es la versión que se citó.
    cur.execute('CREATE SCHEMA IF NOT EXISTS noticias')
    cur.execute("""CREATE TABLE IF NOT EXISTS noticias.notas (nota_id serial PRIMARY KEY, medio text, wp_id bigint,
                   fecha timestamp, url text, titulo text, texto text, categorias text, consultas text, bajada date,
                   via text DEFAULT 'api_wp', captura text, UNIQUE (medio, wp_id))""")
    cur.execute('CREATE INDEX IF NOT EXISTS notas_fecha_idx ON noticias.notas (fecha)')
    cur.execute('CREATE TEMP TABLE _n (medio text, wp_id bigint, fecha timestamp, url text, titulo text, texto text, '
                'categorias text, consultas text, bajada date)')
    buf = io.StringIO()
    df[['medio', 'wp_id', 'fecha', 'url', 'titulo', 'texto', 'categorias', 'consultas', 'bajada']].to_csv(
        buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY _n FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute("""INSERT INTO noticias.notas (medio, wp_id, fecha, url, titulo, texto, categorias, consultas, bajada, via)
                   SELECT medio, wp_id, fecha, url, titulo, texto, categorias, consultas, bajada, 'api_wp' FROM _n
                   ON CONFLICT (medio, wp_id) DO UPDATE SET consultas = EXCLUDED.consultas
                   WHERE notas.consultas IS DISTINCT FROM EXCLUDED.consultas""")
    tocadas = cur.rowcount
    cur.execute("COMMENT ON TABLE noticias.notas IS 'Notas de prensa de Misiones (9 medios y el portal del Gobierno) sobre "
                "cortes, crecidas y evacuados; texto completo sin HTML ni menús (pipeline/noticias/bajar_notas.py, "
                "bajar_sin_api.py)'")
    cur.execute("""CREATE TABLE IF NOT EXISTS noticias.fuentes (medio text PRIMARY KEY, endpoint text, consultas text,
                   notas int, desde timestamp, hasta timestamp)""")
    for m, base in MEDIOS.items():
        cur.execute("""INSERT INTO noticias.fuentes (medio, endpoint, consultas, notas, desde, hasta)
                       SELECT %(m)s, %(e)s, %(q)s, count(*), min(fecha), max(fecha) FROM noticias.notas WHERE medio = %(m)s
                       ON CONFLICT (medio) DO UPDATE SET endpoint = EXCLUDED.endpoint, consultas = EXCLUDED.consultas,
                         notas = EXCLUDED.notas, desde = EXCLUDED.desde, hasta = EXCLUDED.hasta""",
                    {'m': m, 'e': base + '/wp-json/wp/v2/posts', 'q': '; '.join(CONSULTAS)})
    actualizar_fuentes(cur)
    conn.commit()
    fu = pd.read_sql('SELECT medio, notas, desde::date, hasta::date FROM noticias.fuentes ORDER BY notas DESC', conn)
    conn.close()
    log('notas por medio:\n' + fu.to_string(index=False))
    log(f'API: {len(df):,} notas únicas en el crudo; {tocadas:,} nuevas o con consultas nuevas')
    if not respaldo:      # actualizar.py hace su propio zip incremental
        return 0
    # respaldo del crudo en OneDrive
    RESPALDO.mkdir(parents=True, exist_ok=True)
    z = shutil.make_archive(str(RESPALDO / f'crudo_{dt.date.today():%Y%m%d}'), 'zip', DIR / 'crudo')
    log(f'respaldo: {z} ({Path(z).stat().st_size / 1e6:,.1f} MB)')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--solo-cargar', action='store_true')
    a = ap.parse_args()
    if not a.solo_cargar:
        t0 = time.time()
        with ThreadPoolExecutor(len(MEDIOS)) as ex:
            res = dict(zip(MEDIOS, ex.map(lambda kv: bajar_medio(*kv), MEDIOS.items())))
        log(f'bajada completa en {time.time() - t0:.0f} s: ' + ', '.join(f'{m} {sum(v.values()):,}' for m, v in res.items()))
    return cargar()


if __name__ == '__main__':
    sys.exit(main())
