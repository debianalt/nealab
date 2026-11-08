"""Notas de medios sin API abierta y archivo completo de Misiones Online (fase 6b: sólo datos).

Línea que se respeta (decisión del 7-nov-2026): se leen páginas públicas, sitemaps y archivos públicos, con un
User-Agent que dice quiénes somos, a ritmo lento y según robots.txt. NO se engaña ningún control de acceso: no se
resuelve el desafío de Cloudflare ni se finge ser un navegador.

  elterritorio     robots.txt vacío. Direcciones: sitemap.xml (últimas semanas) + índice CDX de la Wayback Machine
                   (www.elterritorio.com.ar/noticias/*); texto: HTML del diario (JSON-LD articleBody).
  lavozdecataratas robots.txt «Disallow:» vacío. Direcciones: 33 post-sitemaps; texto: HTML (contenedor Elementor).
                   La API está cerrada (401), las páginas no.
  misionesonline   el buscador de su API recorta por relevancia: direcciones de sus 625 post-sitemaps (cache por
                   sitemap); texto: API por lotes de 100 slugs (`?slug=a,b,...`).
  misionescuatro   robots.txt permite, pero Cloudflare desafía a todo cliente que no sea un navegador (403) y Common
                   Crawl tampoco pasa: direcciones y texto de las copias de la Wayback Machine (via = wayback, con la
                   fecha de captura). El índice se pide por sección (SECCIONES_M4: las de la portada, sin deportes,
                   espectáculos, cocina, internacionales ni cultura) y con el filtro de palabras del lado del servidor;
                   las páginas ya bajadas del índice de todo el dominio se suman.

Estado (decisión del 7-nov-2026, ver probar_utilidad.py): La Voz y el archivo de Misiones Online, completos; El
Territorio y MisionesCuatro, una muestra al azar de 300 (semilla 11) más lo bajado antes de un corte. No se completan:
aportan 11-18 % de eventos nuevos reales y la prensa sirve a escala de municipio y día, no por volumen.

Filtro: las palabras de la dirección (slug) contra un vocabulario fijo (`pasa()`, v2: agua, o vía con un estado
de corte; La Voz se bajó entera con el amplio v1, FILTRO). Sólo se baja lo que pasa el filtro
y no está ya en noticias.notas. Crudos por nota en %HEXOMA_FUENTES%/noticias/crudo_html/<medio>/, índices en
.../indices/. Reanudable. Al final se cargan a noticias.notas (columnas via y captura) y se actualiza el respaldo.

Uso (desde neahub/):
    python pipeline/noticias/bajar_sin_api.py [--medios elterritorio,lavozdecataratas,misionesonline,misionescuatro]
    python pipeline/noticias/bajar_sin_api.py --solo-cargar
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from bajar_notas import DIR, RESPALDO, texto_plano  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar; investigacion CONICET)'}
PAUSA = {'web.archive.org': 2.0}
PAUSA_DEF = 1.0
CDX = 'https://web.archive.org/cdx/search/cdx'
FILTRO = re.compile(r'(?:^|-)(puente|alcantarill|ruta|camino|intransitable|vialidad|clausur|inhabilit|interrump|'
                    r'corte|cortad|cortan|apagon|arroyo|crecida|crece|desbord|inund|anegad|temporal|tormenta|lluvia|'
                    r'granizo|luz|energia|emsa|evacuad|aislad)|rio-(uruguay|parana|iguazu)|defensa-civil')
# el mismo vocabulario, como filtro del lado del servidor de la Wayback (achica cada página del índice)
FILTRO_CDX = ('original:.*(puente|alcantarill|ruta|camino|intransitable|vialidad|clausur|inhabilit|interrump|corte|'
              'cortad|cortan|apagon|arroyo|crecida|crece|desbord|inund|anegad|temporal|tormenta|lluvia|granizo|luz|'
              'energia|emsa|evacuad|aislad|rio-uruguay|rio-parana|rio-iguazu|defensa-civil).*')
SECCIONES_M4 = ['agro', 'clima', 'el-clima', 'economia', 'general', 'interes-general', 'judiciales', 'municipales',
                'nacionales', 'policiales', 'politica', 'posadas', 'provinciales', 'regionales', 'salud', 'sociedad']
LOTE_MO = 20    # con 100 notas completas por respuesta el servidor corta el pedido
LARGO_MO = 3500   # caracteres de slugs por pedido: con 100 slugs largos el servidor responde 414
MUESTRA: dict = {}   # --muestra N: N notas al azar por medio (prueba de si vale la pena bajar todo)
# Filtro v2 (7-nov-2026): sólo lo concreto y geolocalizable. Medido sobre las 37.161 notas del corpus contra el
# amplio (FILTRO): pasa el 11 % en vez del 35 %, rinde mención geolocalizada de agua o vía en el 49 % (21 %) y
# conserva el 69 % de las notas con altura de río (71 %). Pierde «temporal»/«tormenta» genéricos.
AGUA = re.compile(r'arroyo|(?:^|-)rio-|crecida|crece|creciente|desbord|inund|anegad|evacuad|aislad|altura|'
                  r'hidrometro|(?:^|-)cota|bajante|repunte|caudal')
VIA = re.compile(r'(?:^|-)(?:ruta|rutas|puente|puentes|camino|caminos|alcantarill\w*|vialidad|vado|baden|balsa|paso)(?:-|$)')
ESTADO = re.compile(r'corte|cortad|cortan|intransitable|clausur|inhabilit|interrump|habilit|cayo|caida|cedio|cedi|'
                    r'hundi|rotura|colaps|derrumb|socav|destru|anegad|tapad|cubiert|agua')
# El corte de luz salió el 7-nov-2026 (sin señal contra Black Marble: probar_utilidad.py, prueba B).


def pasa(slug: str) -> bool:
    return bool(AGUA.search(slug) or (VIA.search(slug) and ESTADO.search(slug)))


_ultimo: dict[str, float] = {}
_cerrojo = threading.Lock()


def slug_de(url: str) -> str:
    p = urlparse(url).path.rstrip('/').split('/')[-1]
    p = ''.join(c for c in unicodedata.normalize('NFD', p.lower()) if unicodedata.category(c) != 'Mn')
    return re.sub(r'^\d+-', '', p)


def url_norm(u: str) -> str:
    u = u.strip().replace('http://', 'https://')
    u = re.sub(r'://www\.', '://', u)
    return u.split('?')[0].split('#')[0].rstrip('/').lower()


def pedir(s: requests.Session, url: str, **kw) -> requests.Response | None:
    host = urlparse(url).netloc
    for intento in range(6):
        with _cerrojo:
            espera = _ultimo.get(host, 0) + PAUSA.get(host, PAUSA_DEF) - time.time()
            _ultimo[host] = time.time() + max(espera, 0)
        if espera > 0:
            time.sleep(espera)
        try:
            r = s.get(url, timeout=120, **kw)
        except requests.RequestException:
            time.sleep(15 * (intento + 1))
            continue
        caido = r.status_code == 200 and 'Temporarily Offline' in r.text[:500]
        if r.status_code in (429, 500, 502, 503, 504) or caido:
            time.sleep(30 * (intento + 1))
            continue
        return r
    return None


def locs(s, url) -> list[str]:
    r = pedir(s, url)
    return re.findall(r'<loc>([^<]+)</loc>', r.text) if r is not None and r.status_code == 200 else []


def cdx(s, url, match, cache: Path, filtro: str | None = None) -> list[tuple[str, str]]:
    """(original, timestamp) de capturas 200 HTML de la Wayback Machine, página por página (cache por página)."""
    cache.mkdir(parents=True, exist_ok=True)
    r = pedir(s, CDX, params={'url': url, 'matchType': match, 'filter': 'statuscode:200', 'showNumPages': 'true'})
    filtros = ['statuscode:200', 'mimetype:text/html'] + ([filtro] if filtro else [])
    n = int(r.text.strip()) if r is not None and r.text.strip().isdigit() else 0
    out = []
    for pg in range(n):
        p = cache / f'p{pg:04d}.txt'
        if not p.exists():
            rr = pedir(s, CDX, params={'url': url, 'matchType': match, 'filter': filtros,
                                       'fl': 'original,timestamp', 'collapse': 'urlkey', 'page': pg})
            if rr is None or rr.status_code != 200:
                log(f'    CDX {url} página {pg}: sin respuesta')
                continue
            p.write_text(rr.text, encoding='utf-8')
        for linea in p.read_text(encoding='utf-8').splitlines():
            partes = linea.split()
            if len(partes) == 2:
                out.append((partes[0], partes[1]))
        if pg % 25 == 0:
            log(f'    CDX {url}: página {pg + 1}/{n}, {len(out):,} capturas')
    return out


def paginas_cache(cache: Path) -> list[tuple[str, str]]:
    out = []
    for p in sorted(cache.glob('p*.txt')):
        for linea in p.read_text(encoding='utf-8').splitlines():
            partes = linea.split()
            if len(partes) == 2:
                out.append((partes[0], partes[1]))
    return out


def descubrir(s, medio: str) -> pd.DataFrame:
    idx = DIR / 'indices' / f'{medio}.csv'
    if idx.exists():
        return pd.read_csv(idx, dtype=str)
    filas = []
    if medio == 'elterritorio':
        for sm in locs(s, 'https://www.elterritorio.com.ar/sitemap.xml'):
            filas += [(u, None) for u in locs(s, sm)]
        filas += cdx(s, 'www.elterritorio.com.ar/noticias/', 'prefix', DIR / 'indices' / 'cdx_elterritorio', FILTRO_CDX)
    elif medio == 'lavozdecataratas':
        for sm in locs(s, 'https://lavozdecataratas.com/sitemap_index.xml'):
            if 'post-sitemap' in sm:
                filas += [(u, None) for u in locs(s, sm)]
    elif medio == 'misionesonline':
        mapas = [u for u in locs(s, 'https://misionesonline.net/sitemap.xml') if 'post-sitemap' in u]
        cache = DIR / 'indices' / 'sitemaps_misionesonline'
        cache.mkdir(parents=True, exist_ok=True)
        for k, sm in enumerate(mapas):
            c = cache / (re.sub(r'\W+', '_', sm.rsplit('/', 1)[-1]) + '.txt')
            if not c.exists():
                us = locs(s, sm)
                if not us:
                    continue
                c.write_text('\n'.join(us), encoding='utf-8')
            filas += [(u, None) for u in c.read_text(encoding='utf-8').split()]
            if k % 50 == 0:
                log(f'    misionesonline sitemaps {k + 1}/{len(mapas)}: {len(filas):,} direcciones')
    elif medio == 'misionescuatro':
        filas += paginas_cache(DIR / 'indices' / 'cdx_misionescuatro')   # índice de todo el dominio, hasta donde llegó
        for sec in SECCIONES_M4:
            filas += cdx(s, f'misionescuatro.com/{sec}/', 'prefix', DIR / 'indices' / f'cdx_misionescuatro_{sec}', FILTRO_CDX)
    d = pd.DataFrame(filas, columns=['url', 'captura'])
    d['clave'] = d.url.map(url_norm)
    d = d.sort_values('captura').drop_duplicates('clave', keep='last')
    d.to_csv(idx, index=False)
    return d


def es_nota(medio: str, url: str) -> bool:
    path = urlparse(url).path
    if re.search(r'/(tag|category|categoria|author|autor|page|feed|wp-|amp/?$|seccion)', path):
        return False
    if medio == 'elterritorio':
        return '/noticias/' in path
    return path.count('/') >= 2 and len(slug_de(url)) > 15


def extraer_html(h: str) -> dict:
    from bs4 import BeautifulSoup
    sopa = BeautifulSoup(h, 'lxml')
    titulo = fecha = cuerpo = None
    for sc in sopa.find_all('script', type='application/ld+json'):
        try:
            d = json.loads(sc.string or '')
        except (ValueError, TypeError):
            continue
        objs = d if isinstance(d, list) else d.get('@graph', [d]) if isinstance(d, dict) else []
        for o in objs:
            if not isinstance(o, dict):
                continue
            if o.get('articleBody') and not cuerpo:
                cuerpo = o['articleBody']
            if o.get('@type') in ('NewsArticle', 'Article', 'BlogPosting', 'ReportageNewsArticle'):
                titulo = titulo or o.get('headline')
                fecha = fecha or o.get('datePublished')
    if not titulo:
        m = sopa.find('meta', property='og:title')
        titulo = m['content'] if m and m.get('content') else (sopa.title.string if sopa.title else None)
    if not fecha:
        m = sopa.find('meta', property='article:published_time')
        fecha = m['content'] if m and m.get('content') else None
    if not cuerpo:
        cont = sopa.select_one('[class*="theme-post-content"], .entry-content, .td-post-content, .post-content, '
                               '.article-body, .nota-cuerpo, article')
        if cont is not None:
            for x in cont.select('script, style, figure, aside, .sharedaddy, .related, [class*="relacionad"]'):
                x.decompose()
            cuerpo = '\n'.join(p.get_text(' ', strip=True) for p in cont.find_all(['p', 'h2', 'h3', 'li'])
                               if p.get_text(strip=True))
    def limpio(x):   # El Territorio codifica dos veces las entidades del cuerpo (&amp;ntilde;)
        import html as _h
        x = x or ''
        for _ in range(3):
            y = _h.unescape(x)
            if y == x:
                break
            x = y
        return texto_plano(x)
    return {'titulo': limpio(titulo), 'fecha': fecha, 'texto': limpio(cuerpo)}


def bajar_medio(medio: str, ya: set[str]) -> int:
    s = requests.Session()
    s.headers.update(UA)
    d = descubrir(s, medio)
    d = d[d.url.map(lambda u: es_nota(medio, u)) & d.url.map(lambda u: pasa(slug_de(u)))]
    d = d[~d.clave.isin(ya)]
    log(f'{medio}: {len(d):,} notas a bajar (pasan el filtro y no están en el corpus)')
    if MUESTRA.get('n') and medio in MUESTRA['medios'] and len(d) > MUESTRA['n']:
        d = d.sample(MUESTRA['n'], random_state=MUESTRA['semilla'])
        d[['url', 'captura', 'clave']].to_csv(DIR / 'indices' / f'muestra_{medio}.csv', index=False)
        log(f'{medio}: muestra al azar de {len(d):,} (semilla {MUESTRA["semilla"]})')
    dest = DIR / 'crudo_html' / medio
    dest.mkdir(parents=True, exist_ok=True)
    hechas = 0
    if medio == 'misionesonline':
        return bajar_mo(s, d, dest, len(d))
    for r in d.itertuples():
        p = dest / (hashlib.sha1(r.clave.encode()).hexdigest()[:20] + '.json')
        if p.exists():
            continue
        fuente = (f'https://web.archive.org/web/{r.captura}id_/{r.url}' if medio == 'misionescuatro' else r.url)
        rr = pedir(s, fuente)
        if rr is None or rr.status_code != 200:
            continue
        rr.encoding = rr.apparent_encoding if rr.encoding in (None, 'ISO-8859-1') else rr.encoding
        reg = dict(extraer_html(rr.text), url=r.url, wp_id=None,
                   via='wayback' if medio == 'misionescuatro' else 'html',
                   captura=r.captura if medio == 'misionescuatro' else None)
        if not reg['fecha']:
            m = re.search(r'/(20\d\d)/(\d\d)/(\d\d)/', r.url)
            reg['fecha'] = f'{m.group(1)}-{m.group(2)}-{m.group(3)}' if m else None
        p.write_text(json.dumps(reg, ensure_ascii=False), encoding='utf-8')
        hechas += 1
        if hechas % 200 == 0:
            log(f'  {medio}: {hechas:,}/{len(d):,}')
    return hechas


def seg(url: str) -> str:
    return unquote(urlparse(url).path.rstrip('/').split('/')[-1]).lower()


def bajar_mo(s, d: pd.DataFrame, dest: Path, total: int) -> int:
    """Misiones Online: la API acepta varios slugs separados por coma; LOTE_MO por pedido."""
    d = d.assign(p=d.clave.map(lambda c: dest / (hashlib.sha1(c.encode()).hexdigest()[:20] + '.json')))
    d = d[~d.p.map(Path.exists)]
    hechas = 0
    lotes, act, largo = [], [], 0
    for u, p in zip(d.url, d.p):
        sg = seg(u)
        if act and (len(act) == LOTE_MO or largo + len(quote(sg)) > LARGO_MO):
            lotes.append(act)
            act, largo = [], 0
        act.append((sg, p))
        largo += len(quote(sg)) + 1
    if act:
        lotes.append(act)
    hechos_lotes = 0
    for k, lote in enumerate(lotes):
        por_slug = dict(lote)
        hechos_lotes += len(lote)
        rr = pedir(s, 'https://misionesonline.net/wp-json/wp/v2/posts',
                   params={'slug': ','.join(por_slug), 'per_page': LOTE_MO,
                           '_fields': 'id,date,slug,link,title,content,categories'})
        if rr is None or rr.status_code != 200:
            log(f'  misionesonline: lote {k} sin respuesta ({None if rr is None else rr.status_code})')
            continue
        for a in rr.json():
            p = por_slug.get(unquote(a['slug']).lower())
            if p is None:
                continue
            reg = {'url': a['link'], 'wp_id': a['id'], 'fecha': a['date'], 'titulo': texto_plano(a['title']['rendered']),
                   'texto': texto_plano(a['content']['rendered']), 'via': 'api_wp', 'captura': None}
            p.write_text(json.dumps(reg, ensure_ascii=False), encoding='utf-8')
            hechas += 1
        if k % 20 == 0:
            log(f'  misionesonline: {hechos_lotes:,}/{len(d):,} pedidas, {hechas:,} bajadas')
    return hechas


def cargar() -> int:
    filas = []
    for medio_dir in sorted((DIR / 'crudo_html').glob('*')):
        for p in medio_dir.glob('*.json'):
            r = json.loads(p.read_text(encoding='utf-8'))
            if not r.get('texto') or len(r['texto']) < 200:
                continue
            filas.append(dict(r, medio=medio_dir.name))
    df = pd.DataFrame(filas)
    # formatos mezclados («…+00:00» y sin zona): sin format='ISO8601' pandas infiere el de la primera y anula el resto
    df['fecha'] = pd.to_datetime(df.fecha, errors='coerce', utc=True, format='ISO8601').dt.tz_convert(None)
    df['wp_id'] = pd.to_numeric(df.wp_id, errors='coerce')
    # medios sin wp_id: un id estable a partir de la dirección (negativo, para no chocar con los de WordPress)
    sin = df.wp_id.isna()
    df.loc[sin, 'wp_id'] = df.loc[sin, 'url'].map(lambda u: -int(hashlib.sha1(url_norm(u).encode()).hexdigest()[:12], 16))
    df['wp_id'] = df.wp_id.astype('int64')
    df = df.drop_duplicates(['medio', 'wp_id'])
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('ALTER TABLE noticias.notas ADD COLUMN IF NOT EXISTS via text DEFAULT \'api_wp\'')
    cur.execute('ALTER TABLE noticias.notas ADD COLUMN IF NOT EXISTS captura text')
    cur.execute('CREATE TEMP TABLE _n (medio text, wp_id bigint, fecha timestamp, url text, titulo text, texto text, '
                'via text, captura text)')
    buf = io.StringIO()
    df[['medio', 'wp_id', 'fecha', 'url', 'titulo', 'texto', 'via', 'captura']].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY _n FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""INSERT INTO noticias.notas (medio, wp_id, fecha, url, titulo, texto, categorias, consultas, bajada, via, captura)
                   SELECT medio, wp_id, fecha, url, titulo, texto, '[]', 'sitemap/wayback', current_date, via, captura FROM _n
                   ON CONFLICT (medio, wp_id) DO UPDATE SET fecha = coalesce(notas.fecha, EXCLUDED.fecha)
                   WHERE notas.fecha IS NULL""")
    nuevas = cur.rowcount
    cur.execute("""INSERT INTO noticias.fuentes (medio, endpoint, consultas, notas, desde, hasta)
                   SELECT medio, CASE medio WHEN 'misionescuatro' THEN 'web.archive.org (copias)' ELSE 'sitemaps + HTML' END,
                          'filtro por la dirección (bajar_sin_api.py)', count(*), min(fecha), max(fecha)
                   FROM noticias.notas GROUP BY medio
                   ON CONFLICT (medio) DO UPDATE SET notas = EXCLUDED.notas, desde = EXCLUDED.desde, hasta = EXCLUDED.hasta""")
    conn.commit()
    conn.close()
    log(f'cargadas {nuevas:,} notas nuevas; por medio: {df.medio.value_counts().to_dict()}')
    z = shutil.make_archive(str(RESPALDO / f'crudo_html_{dt.date.today():%Y%m%d}'), 'zip', DIR / 'crudo_html')
    log(f'respaldo: {z} ({Path(z).stat().st_size / 1e6:,.1f} MB)')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--medios', default='elterritorio,lavozdecataratas,misionesonline,misionescuatro')
    ap.add_argument('--solo-cargar', action='store_true')
    ap.add_argument('--muestra', type=int, default=0, help='N notas al azar por medio (sólo elterritorio y misionescuatro)')
    ap.add_argument('--semilla', type=int, default=11)
    a = ap.parse_args()
    if a.muestra:
        MUESTRA.update(n=a.muestra, semilla=a.semilla, medios={'elterritorio', 'misionescuatro'})
    if not a.solo_cargar:
        conn = connect(DB)
        ya = set(pd.read_sql('SELECT url FROM noticias.notas', conn).url.map(url_norm))
        conn.close()
        medios = a.medios.split(',')
        with ThreadPoolExecutor(len(medios)) as ex:
            res = dict(zip(medios, ex.map(lambda m: bajar_medio(m, ya), medios)))
        log(f'bajadas: {res}')
    return cargar()


if __name__ == '__main__':
    sys.exit(main())
