"""Archivo de notas de prensa de Misiones sobre cortes de puentes, caminos y luz (fase 6: sólo datos).

Medios con la API de WordPress abierta (verificado el 7-nov-2026): /wp-json/wp/v2/posts?search=<consulta>, paginado
de a 100. Para cada medio y cada consulta de CONSULTAS se bajan TODAS las páginas (texto completo). User-Agent
honesto y una pausa de PAUSA_S entre pedidos al mismo medio; los medios corren en paralelo.

Fuera: El Territorio (no es WordPress), MisionesCuatro (desafío anti-bots de Cloudflare: no se sortea),
La Voz de Cataratas (API cerrada, 401).

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
]


def slug(s: str) -> str:
    frase = s.startswith('"')
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return ('frase_' if frase else '') + re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


def bajar_medio(medio: str, base: str) -> dict:
    s = requests.Session()
    s.headers.update(UA)
    resumen = {}
    for q in CONSULTAS:
        d = DIR / 'crudo' / medio
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
                            '_fields': 'id,date,link,title,content,excerpt,categories'})
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


def cargar() -> int:
    filas = {}
    vigentes = {slug(q): q for q in CONSULTAS}   # crudos de consultas descartadas (p. ej. «vado») no entran
    for medio in MEDIOS:
        for p in sorted((DIR / 'crudo' / medio).glob('*_p*.json')):
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
    df['bajada'] = dt.date.today().isoformat()
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('CREATE SCHEMA IF NOT EXISTS noticias')
    cur.execute('DROP TABLE IF EXISTS noticias.notas CASCADE')
    cur.execute("""CREATE TABLE noticias.notas (nota_id serial PRIMARY KEY, medio text, wp_id bigint, fecha timestamp,
                   url text, titulo text, texto text, categorias text, consultas text, bajada date,
                   UNIQUE (medio, wp_id))""")
    buf = io.StringIO()
    df[['medio', 'wp_id', 'fecha', 'url', 'titulo', 'texto', 'categorias', 'consultas', 'bajada']].to_csv(
        buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY noticias.notas (medio, wp_id, fecha, url, titulo, texto, categorias, consultas, bajada) '
                    'FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute('CREATE INDEX ON noticias.notas (fecha)')
    cur.execute("COMMENT ON TABLE noticias.notas IS 'Notas de prensa de Misiones (API WordPress de 6 medios) traídas por "
                "consultas de cortes de puentes, caminos y luz; texto completo sin HTML (pipeline/noticias/bajar_notas.py)'")
    fu = df.groupby('medio').agg(notas=('wp_id', 'size'), desde=('fecha', 'min'), hasta=('fecha', 'max')).reset_index()
    fu['endpoint'] = fu.medio.map(lambda m: MEDIOS[m] + '/wp-json/wp/v2/posts')
    fu['consultas'] = '; '.join(CONSULTAS)
    cur.execute('DROP TABLE IF EXISTS noticias.fuentes')
    cur.execute('CREATE TABLE noticias.fuentes (medio text PRIMARY KEY, endpoint text, consultas text, notas int, '
                'desde timestamp, hasta timestamp)')
    buf = io.StringIO()
    fu[['medio', 'endpoint', 'consultas', 'notas', 'desde', 'hasta']].to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY noticias.fuentes FROM STDIN WITH (FORMAT csv)', buf)
    conn.commit()
    conn.close()
    log('notas por medio:\n' + fu[['medio', 'notas', 'desde', 'hasta']].to_string(index=False))
    log(f'noticias.notas: {len(df):,} notas únicas')
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
