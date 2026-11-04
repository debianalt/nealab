"""Sonda de textos: panel de cada capa en es/en/pt/gn y páginas de texto en sus cuatro rutas.

Uso: python director/sondas/textos.py BASE OUT [--capas x,y]
Chequea frases prohibidas, claves i18n visibles, nombres de columna, castellano que
quedó sin traducir en en/pt, links internos rotos y hreflang.
"""
import argparse, asyncio, json, re, sys, time
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from navegador import abrir_navegador, contexto_spatia
from comun import Hallazgos, revisar_texto, reglas

ESTADO_JS = (AQUI / 'estado.js').read_text(encoding='utf-8')
IDIOMAS = ['es', 'en', 'pt', 'gn']
PAGINAS = ['/servicios', '/metodologia', '/terminos', '/eudr/check']
# Palabras que sólo aparecen en castellano (no en inglés ni en portugués).
CASTELLANO = re.compile(r'(¿|\b(los|del|las|qué|cómo|dónde|también|según|hogares|ningún|año|años)\b)', re.I)
PERMITIDO_EN_CASTELLANO = re.compile(r'(Misiones|Corrientes|Formosa|Chaco|Itapúa|Alto Paraná|San Pedro|Capital|Oberá|Posadas|Ruta|del Este|de Mayo)')


def castellano_sobrante(texto):
    """Fragmentos con castellano en una vista en/pt (sin nombres propios de lugares)."""
    out = []
    for linea in texto.split('\n'):
        l2 = PERMITIDO_EN_CASTELLANO.sub('', linea)
        if len(l2.strip()) > 3 and CASTELLANO.search(l2):
            out.append(linea.strip()[:120])
    return out


async def abrir_desplegables(page):
    for sel in ['details:not([open]) > summary', '.accordion-header', 'button[aria-expanded="false"]']:
        for el in await page.query_selector_all(sel):
            try: await el.click(timeout=1500); await page.wait_for_timeout(150)
            except Exception: pass


async def panel_capa(br, base, lente, capa, H):
    """Una carga por capa; el idioma se cambia en la página (i18n.setLocale), como el botón ES/EN/GN/BR."""
    cx = await contexto_spatia(br, base)
    page = await cx.new_page()
    url = f'{base}/?lens={lente}&a={capa}'
    try:
        await page.goto(url, wait_until='domcontentloaded')
        t0 = time.time()
        while time.time() - t0 < 120:
            e = await page.evaluate(ESTADO_JS, {})
            if e.get('analisis') == capa and not e.get('cargando') and e.get('leyenda'): break
            await page.wait_for_timeout(1000)
        else:
            H.add('no_carga', 'alta', {'capa': capa, 'vista': 'panel', 'url': url}, 'El panel de la capa no cargó en 120 s.'); return
        for idioma in IDIOMAS:
            ctx = {'capa': capa, 'idioma': idioma, 'vista': 'panel', 'url': url}
            await page.evaluate("(l) => window.__spatia.i18n.setLocale(l)", idioma)
            await page.wait_for_timeout(1200)
            await abrir_desplegables(page); await page.wait_for_timeout(600)
            e = await page.evaluate(ESTADO_JS, {})
            texto = (e.get('textoPanel') or '') + chr(10) + ((e.get('leyenda') or {}).get('texto') or '')
            H.revisar(revisar_texto(texto), ctx, 'panel')
            if idioma in ('en', 'pt'):
                sob = castellano_sobrante(texto)
                if sob:
                    H.add('sin_traducir', 'baja', ctx, f'{len(sob)} líneas en castellano en la vista {idioma}: ' + ' / '.join(f'«{x}»' for x in sob[:4]),
                          clave='sin_traducir')
    finally:
        await cx.close()


async def pagina(br, base, ruta, idioma, H, links_vistos):
    cx = await contexto_spatia(br, base)
    page = await cx.new_page()
    pref = '' if idioma == 'es' else f'/{idioma}'
    url = f'{base}{pref}{ruta}'
    ctx = {'pagina': ruta, 'idioma': idioma, 'vista': 'pagina', 'url': url}
    errores = []
    page.on('console', lambda m: errores.append(m.text) if m.type == 'error' else None)
    page.on('pageerror', lambda x: errores.append('PAGEERROR ' + str(x)))
    try:
        r = await page.goto(url, wait_until='networkidle', timeout=60000)
        if not r or r.status >= 400:
            H.add('pagina_rota', 'alta', ctx, f'{url} responde {r.status if r else "sin respuesta"}'); return
        await abrir_desplegables(page)
        texto = await page.evaluate('() => document.body.innerText')
        H.revisar(revisar_texto(texto, interfaz=False), ctx, 'pagina')
        if idioma in ('en', 'pt') and ruta not in ('/terminos',):
            sob = castellano_sobrante(texto)
            if len(sob) > 3:
                H.add('sin_traducir', 'baja', ctx, f'{len(sob)} líneas en castellano en {url}: ' + ' / '.join(f'«{s}»' for s in sob[:4]), clave='sin_traducir')
        hl = await page.evaluate("() => [...document.querySelectorAll('link[rel=alternate][hreflang]')].map(l => l.hreflang)")
        if len(set(hl)) < 4:
            H.add('hreflang', 'baja', ctx, f'hreflang incompleto en {url}: {sorted(set(hl))}', clave='hreflang')
        # Links internos: se piden desde la página (el navegador pasa la protección de Cloudflare)
        hrefs = await page.evaluate("() => [...document.querySelectorAll('a[href]')].map(a => a.href)")
        host = urlparse(base).netloc
        nuevos = sorted({h.split('#')[0] for h in hrefs if urlparse(h).netloc == host} - links_vistos)
        links_vistos.update(nuevos)
        if nuevos:
            est = await page.evaluate("""async (us) => Promise.all(us.map(u => fetch(u, {method: 'GET', redirect: 'follow'})
                .then(r => [u, r.status]).catch(e => [u, 0])))""", nuevos[:60])
            for u, s in est:
                if s >= 400 or s == 0:
                    H.add('link_roto', 'media', {**ctx, 'link': u}, f'Link interno roto en {url}: {u} → {s}', clave=u)
        H.consola(errores, ctx)
    except Exception as ex:
        H.add('sonda_fallo', 'media', ctx, f'{type(ex).__name__}: {str(ex)[:200]}')
    finally:
        await cx.close()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('base'); ap.add_argument('out'); ap.add_argument('--capas')
    ap.add_argument('--solo', choices=['paginas', 'paneles'])
    args = ap.parse_args()
    base = args.base.rstrip('/'); out = Path(args.out)
    H = Hallazgos(out, 'textos')
    async with async_playwright() as p:
        br = await abrir_navegador(p)
        cx = await contexto_spatia(br, base); page = await cx.new_page()
        await page.goto(base + '/', wait_until='domcontentloaded')
        await page.wait_for_function('() => window.__spatia && window.__spatia.registros', timeout=90000)
        capas = await page.evaluate("() => window.__spatia.registros.ANALYSIS_REGISTRY.filter(a => !a.status || a.status === 'available').map(a => [a.lensId, a.id])")
        await cx.close()
        if args.capas: capas = [c for c in capas if c[1] in args.capas.split(',')]
        links = set()
        for ruta in ([] if args.solo == 'paneles' else PAGINAS + [f'/metodologia/{c}' for _, c in capas]):
            for idioma in IDIOMAS:
                await pagina(br, base, ruta, idioma, H, links)
                print(f'página {idioma}{ruta}: {H.total()} hallazgos', flush=True)
        for lente, capa in ([] if args.solo == 'paginas' else capas):
            try:
                await panel_capa(br, base, lente, capa, H)
            except Exception as ex:
                H.add('sonda_fallo', 'media', {'capa': capa, 'vista': 'panel'}, f'{type(ex).__name__}: {str(ex)[:200]}')
            print(f'panel {capa}: {H.total()} hallazgos', flush=True)
        await br.close()
    H.guardar()


if __name__ == '__main__':
    asyncio.run(main())
