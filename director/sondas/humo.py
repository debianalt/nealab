"""Prueba de humo: ¿el navegador del runner dibuja el mapa de Spatia?

Uso: python director/sondas/humo.py BASE_URL OUT_DIR
Sale con 1 si no hay WebGL o si la captura no tiene hexágonos pintados.
"""
import sys, asyncio, json, os
from playwright.async_api import async_playwright
from navegador import abrir_navegador, contexto_spatia

BASE, OUT = sys.argv[1].rstrip('/'), sys.argv[2]
os.makedirs(OUT, exist_ok=True)


async def main():
    async with async_playwright() as p:
        br = await abrir_navegador(p)
        ctx = await contexto_spatia(br, BASE)
        page = await ctx.new_page()
        errores = []
        page.on('console', lambda m: errores.append(m.text) if m.type == 'error' else None)
        gl = await page.evaluate("""() => { const c = document.createElement('canvas');
            const g = c.getContext('webgl2') || c.getContext('webgl'); if (!g) return null;
            const d = g.getExtension('WEBGL_debug_renderer_info');
            return d ? g.getParameter(d.UNMASKED_RENDERER_WEBGL) : 'webgl sin info'; }""")
        print('WebGL:', gl)
        await page.goto(f'{BASE}/?lens=ambiente&a=flood_risk', wait_until='domcontentloaded')
        await page.wait_for_selector('canvas.maplibregl-canvas', timeout=60000)
        await page.wait_for_timeout(4000)
        await page.wait_for_selector('.loading-overlay', state='detached', timeout=120000)
        await page.wait_for_timeout(8000)
        shot = os.path.join(OUT, 'humo.png')
        await page.screenshot(path=shot)
        n = await page.evaluate("""() => { const m = window.__spatia?.map; if (!m) return -1;
            return m.queryRenderedFeatures({layers:['hex-fill']}).length; }""")
        leyenda = await page.evaluate("() => document.querySelector('.legend')?.innerText || ''")
        print('features hex-fill:', n, '(-1 = sin gancho __spatia)')
        print('leyenda:', leyenda.replace('\n', ' | ')[:200])
        print('errores de consola:', json.dumps(errores[:5], ensure_ascii=False))
        await br.close()
        from PIL import Image
        im = Image.open(shot).convert('RGB').resize((320, 200))
        px = list(im.getdata())
        # pixeles saturados (los hexágonos de inundación son azules/tierra, el fondo claro es casi gris)
        sat = sum(1 for r, g, b in px if max(r, g, b) - min(r, g, b) > 60)
        print(f'pixeles saturados: {sat}/{len(px)}')
        sys.exit(0 if gl and sat > len(px) * 0.02 else 1)

asyncio.run(main())
