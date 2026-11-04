"""Sondas de los rincones que la matriz principal no recorre.

Uso: python director/sondas/extras.py BASE OUT --solo temporal|oscuro|movil|ficha [--capas x,y]

  temporal: capas temporales en línea de base y en cambio (tm=baseline|delta) y la escala
            regional de PM2.5 (escala=regional): leyenda, dominio de color, rampa, tooltip.
  oscuro:   cada capa en tema oscuro (Misiones, departamento): leyenda contra pintura y tooltip.
  movil:    cada capa en 390×844 táctil (Misiones, provincia): desborde horizontal, leyenda
            dentro de pantalla y sin tapar el mapa, panel alcanzable, textos.
  ficha:    ?h3=<celda> para celdas de varias capas: tarjetas sin errores ni NaN y el valor de
            la capa de origen igual al del hexágono.
"""
import argparse, asyncio, json, sys, time
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from navegador import abrir_navegador, contexto_spatia
from comun import Hallazgos, reglas, revisar_texto, numeros
from mapa import (estado, esperar_lista, mapa_quieto, matriz, revisar_comun, revisar_hex,
                  revisar_tooltip, revisar_provincia, nueva_pagina)


async def registros(br, base):
    cx = await contexto_spatia(br, base); page = await cx.new_page()
    await page.goto(base + '/', wait_until='domcontentloaded', timeout=120000)
    await page.wait_for_function('() => window.__spatia && window.__spatia.registros', timeout=120000)
    reg = await page.evaluate("""() => { const r = window.__spatia.registros; return {
        ANALYSIS_REGISTRY: r.ANALYSIS_REGISTRY.map(a => ({ id: a.id, lensId: a.lensId, coverage: a.coverage, status: a.status, spatialUnit: a.spatialUnit })),
        capas: Object.fromEntries(Object.values(r.HEX_LAYER_REGISTRY).map(l => [l.id, { temporal: !!l.temporal, regional: l.regionalColorDomain ?? null }])) } }""")
    await cx.close()
    return reg


async def vista_depto(page, base, t, lente, capa, extra=''):
    """Carga provincia para conocer el primer departamento del ranking y abre ese departamento."""
    q = f'/?t={t}&lens={lente}&a={capa}'
    await page.goto(base + q, wait_until='domcontentloaded', timeout=120000)
    e, dt = await esperar_lista(page, capa)
    if dt is None: return None, base + q, None
    dept = (e.get('ranking') or [{}])[0].get('nombre') or ((e.get('deptFill') or {}).get('valores') or [{}])[0].get('name')
    if not dept: return None, base + q, None
    url = base + q + f'&dept={quote(dept)}' + extra
    await page.goto(url, wait_until='domcontentloaded', timeout=120000)
    e, dt = await esperar_lista(page, capa, dept=dept, con_hex=True, timeout=200)
    if dt is None: return None, url, dept
    await mapa_quieto(page, 15000)
    await page.wait_for_timeout(2500)  # tm y escala se aplican después del primer pintado
    return await estado(page, muestra_hover=4), url, dept


async def temporal(br, base, reg, H, capas_f, errores):
    cx = await contexto_spatia(br, base)
    for t in ('misiones', 'corrientes'):
        for (tt, lente, capa) in matriz(reg, [t], None, capas_f):
            info = reg['capas'].get(capa) or {}
            modos = (['baseline', 'delta'] if info.get('temporal') else []) + (['regional'] if info.get('regional') else [])
            for modo in modos:
                extra = '&escala=regional' if modo == 'regional' else f'&tm={modo}'
                page = await nueva_pagina(cx, errores)
                ctx = {'territorio': t, 'capa': capa, 'vista': f'departamento-{modo}'}
                try:
                    e, url, dept = await vista_depto(page, base, t, lente, capa, extra)
                    ctx['url'] = url
                    if not e:
                        H.add('no_carga', 'alta', ctx, f'La vista {modo} no cargó.'); await H.captura(page, ctx); continue
                    if modo in ('baseline', 'delta') and e.get('modoTemporal') != modo:
                        H.add('url_ignorada', 'media', ctx, f'?tm={modo} en la URL pero la página quedó en «{e.get("modoTemporal")}».')
                    revisar_comun(H, e, ctx); revisar_hex(H, e, ctx); await revisar_tooltip(H, page, e, ctx)
                    hx, dom = e.get('hex') or {}, e.get('dominio')
                    # Dominio de color: ¿cubre los valores que se pintan en este modo?
                    if dom and hx.get('n') and modo != 'delta':
                        lo, hi = dom; rango = (hi - lo) or 1
                        if hx['max'] < lo + rango * 0.1 or hx['min'] > hi - rango * 0.1 or hx['max'] < lo or hx['min'] > hi:
                            H.add('dominio_desfasado', 'alta', ctx,
                                  f"En modo {modo} el dominio de color es {lo:g}–{hi:g} pero los valores pintados van de {hx['min']:g} a {hx['max']:g}: casi todo queda en un extremo de la rampa.")
                    if modo == 'regional':
                        esperado = info['regional']
                        ns = numeros(' '.join((e.get('leyenda') or {}).get('extremos') or []))
                        if not any(abs(n - esperado[0]) < 0.06 for n in ns) or not any(abs(n - esperado[1]) < 0.06 for n in ns):
                            H.add('escala_regional', 'alta', ctx, f"Escala regional: la leyenda muestra {ns} y el rango fijo es {esperado}.")
                    H.consola(errores, ctx); await H.captura_si(page, ctx)
                except Exception as ex:
                    H.add('sonda_fallo', 'media', ctx, f'{type(ex).__name__}: {str(ex)[:300]}')
                finally:
                    await page.close()
                H.guardar(); print(f'temporal {t} {capa} {modo}: {H.total()} hallazgos', flush=True)
    await cx.close()


async def oscuro(br, base, reg, H, capas_f, errores):
    cx = await contexto_spatia(br, base, fondo='oscuro')
    celdas = []
    for (t, lente, capa) in matriz(reg, ['misiones'], None, capas_f):
        page = await nueva_pagina(cx, errores)
        ctx = {'territorio': t, 'capa': capa, 'vista': 'departamento-oscuro'}
        try:
            e, url, dept = await vista_depto(page, base, t, lente, capa)
            ctx['url'] = url
            if not e:
                H.add('no_carga', 'alta', ctx, 'La vista en tema oscuro no cargó.'); await H.captura(page, ctx); continue
            tema = await page.evaluate('() => document.documentElement.dataset.theme')
            if tema != 'oscuro':
                H.add('tema_ignorado', 'media', ctx, f'spatia_fondo=oscuro pero la página quedó en tema «{tema}».')
            revisar_comun(H, e, ctx); revisar_hex(H, e, ctx); await revisar_tooltip(H, page, e, ctx)
            H.consola(errores, ctx); await H.captura_si(page, ctx)
        except Exception as ex:
            H.add('sonda_fallo', 'media', ctx, f'{type(ex).__name__}: {str(ex)[:300]}')
        finally:
            await page.close()
        H.guardar(); print(f'oscuro {capa}: {H.total()} hallazgos', flush=True)
    await cx.close()


async def movil(br, base, reg, H, capas_f, errores):
    cx = await contexto_spatia(br, base, movil=True)
    for (t, lente, capa) in matriz(reg, ['misiones'], None, capas_f):
        page = await nueva_pagina(cx, errores)
        url = f'{base}/?t={t}&lens={lente}&a={capa}'
        ctx = {'territorio': t, 'capa': capa, 'vista': 'movil', 'url': url}
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=120000)
            e, dt = await esperar_lista(page, capa)
            if dt is None:
                H.add('no_carga', 'alta', ctx, 'La capa no cargó en el celular (390×844).'); await H.captura(page, ctx); continue
            g = await page.evaluate("""() => {
                const W = innerWidth, Hh = innerHeight, r = el => el && el.getBoundingClientRect();
                const leg = r(document.querySelector('.legend')), side = r(document.querySelector('.sidebar'));
                const anchos = [...document.querySelectorAll('body *')].filter(el => { const b = el.getBoundingClientRect();
                    return b.width > 0 && b.right > W + 2 && getComputedStyle(el).position !== 'fixed' && !el.closest('.maplibregl-canvas-container, .sidebar'); })
                    .slice(0, 5).map(el => (el.className && typeof el.className === 'string' ? '.' + el.className.split(' ')[0] : el.tagName) + ' ' + Math.round(el.getBoundingClientRect().right) + 'px');
                return { W, H: Hh, scrollW: document.documentElement.scrollWidth,
                    leyenda: leg ? { x: leg.left, y: leg.top, w: leg.width, h: leg.height } : null,
                    panel: side ? { x: side.left, w: side.width, h: side.height } : null, anchos } }""")
            if g['scrollW'] > g['W'] + 2:
                H.add('movil_desborde', 'media', ctx, f"La página es más ancha que el celular ({g['scrollW']} px en {g['W']}): {', '.join(g['anchos']) or 'elemento sin identificar'}.")
            lg = g['leyenda']
            if not lg:
                H.add('movil_leyenda', 'media', ctx, 'No hay leyenda visible en el celular.')
            else:
                if lg['x'] < -2 or lg['y'] < -2 or lg['x'] + lg['w'] > g['W'] + 2 or lg['y'] + lg['h'] > g['H'] + 2:
                    H.add('movil_leyenda', 'media', ctx, f"La leyenda sale de la pantalla ({lg['x']:.0f},{lg['y']:.0f} {lg['w']:.0f}×{lg['h']:.0f} en {g['W']}×{g['H']}).")
                if lg['w'] * lg['h'] > g['W'] * g['H'] * 0.35:
                    H.add('movil_leyenda', 'baja', ctx, f"La leyenda tapa {100 * lg['w'] * lg['h'] / (g['W'] * g['H']):.0f} % de la pantalla.")
            revisar_comun(H, e, ctx); revisar_provincia(H, e, ctx)
            H.consola(errores, ctx); await H.captura_si(page, ctx)
        except Exception as ex:
            H.add('sonda_fallo', 'media', ctx, f'{type(ex).__name__}: {str(ex)[:300]}')
        finally:
            await page.close()
        H.guardar(); print(f'movil {capa}: {H.total()} hallazgos', flush=True)
    await cx.close()


async def ficha(br, base, reg, H, capas_f, errores):
    """Toma una celda de varias capas (Misiones y Corrientes) y abre su ficha."""
    cx = await contexto_spatia(br, base)
    capas = [c for c in matriz(reg, ['misiones', 'corrientes'], None, capas_f)]
    elegidas = [capas[i] for i in range(0, len(capas), max(1, len(capas) // 5))][:5]
    for (t, lente, capa) in elegidas:
        page = await nueva_pagina(cx, errores)
        ctx = {'territorio': t, 'capa': capa, 'vista': 'ficha'}
        try:
            e, url, dept = await vista_depto(page, base, t, lente, capa)
            muestra = [s for s in (e or {}).get('enPantalla') or [] if str(s.get('h3') or '').startswith('89') and s.get('value') is not None and s.get('nodata') not in (True, 'true')]
            if not muestra: continue
            cel = muestra[0]; v = e.get('variable') or {}
            url = f"{base}/?h3={cel['h3']}"; ctx['url'] = url
            await page.goto(url, wait_until='domcontentloaded', timeout=120000)
            try:
                await page.wait_for_selector('.fi-card', timeout=120000)
                await page.wait_for_function("!document.querySelector('.fi-state')", timeout=120000)
            except Exception:
                H.add('ficha_no_carga', 'alta', ctx, f"La ficha de {cel['h3']} no terminó de cargar."); await H.captura(page, ctx); continue
            await page.wait_for_timeout(1500)
            f = await page.evaluate("""() => ({ errores: [...document.querySelectorAll('.fi-error, .fi-card .fi-muted')].filter(el => /error al cargar|error loading|erro ao carregar/i.test(el.innerText)).length,
                conError: [...document.querySelectorAll('.fi-card')].filter(c => /error al cargar|error loading|erro ao carregar/i.test(c.innerText)).map(c => c.querySelector('.fi-layer-title')?.innerText.trim()),
                tarjetas: [...document.querySelectorAll('.fi-card')].map(c => ({ titulo: c.querySelector('.fi-layer-title')?.innerText.trim(), texto: c.innerText })) })""")
            if f['errores']:
                H.add('ficha_error', 'alta', ctx, f"La ficha de {cel['h3']} muestra «error al cargar datos» en {f['errores']} tarjetas: {', '.join(x for x in f['conError'] if x)}.")
            for c in f['tarjetas']:
                H.revisar(revisar_texto(c['texto']), {**ctx, 'grafico': c['titulo']}, 'ficha')
            # La tarjeta de la capa de origen tiene que mostrar el valor del hexágono
            esperado = float(cel['value']) * (v.get('scale') or 1)
            titulo = (e.get('leyenda') or {}).get('titulo', '').split('·')[0].strip().lower()
            tarjeta = next((c for c in f['tarjetas'] if c['titulo'] and c['titulo'].lower()[:25] in titulo), None)
            if tarjeta and not (e.get('clases') or {}).get('nominal') and not (e.get('clases') or {}).get('ordinal'):
                ns = numeros(tarjeta['texto'])
                if ns and min(abs(n - esperado) for n in ns) > 0.051 + abs(esperado) * 0.01:
                    H.add('ficha_valor', 'alta', ctx, f"Ficha de {cel['h3']}: el mapa pinta {esperado:.2f} ({v.get('col')}) y la tarjeta «{tarjeta['titulo']}» no muestra ese valor: «{tarjeta['texto'][:160]}».")
            H.consola(errores, ctx); await H.captura_si(page, ctx)
        except Exception as ex:
            H.add('sonda_fallo', 'media', ctx, f'{type(ex).__name__}: {str(ex)[:300]}')
        finally:
            await page.close()
        H.guardar(); print(f'ficha {capa}: {H.total()} hallazgos', flush=True)
    await cx.close()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('base'); ap.add_argument('out')
    ap.add_argument('--solo', required=True, choices=['temporal', 'oscuro', 'movil', 'ficha'])
    ap.add_argument('--capas')
    args = ap.parse_args()
    base = args.base.rstrip('/'); out = Path(args.out)
    H = Hallazgos(out, f'extras_{args.solo}')
    R = reglas()
    capas_f = set(args.capas.split(',')) if args.capas else None
    errores = []
    async with async_playwright() as p:
        br = await abrir_navegador(p)
        reg = await registros(br, base)
        reg['ANALYSIS_REGISTRY'] = [a for a in reg['ANALYSIS_REGISTRY'] if (a.get('spatialUnit') or 'hexagon') == 'hexagon' or args.solo == 'movil']
        await {'temporal': temporal, 'oscuro': oscuro, 'movil': movil, 'ficha': ficha}[args.solo](br, base, reg, H, capas_f, errores)
        await br.close()
    H.guardar()


if __name__ == '__main__':
    asyncio.run(main())
