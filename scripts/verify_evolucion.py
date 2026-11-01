"""Evolución (FlowChart) selection → «Guardar como zona» bar visible (1-nov-2026).

Usage:
    python scripts/verify_evolucion.py BASE_URL OUT_DIR

For deforestation and PM2.5 in Guaraní (Misiones) and the temporal census in
Capital: clicks the widest flow of «Evolución» and checks that the selection bar
appears inside the window (879ace9: it used to render below «Gráficos», off
screen), then saves it as a zone. Same CDN proxy as verify_zones.py.
Run with PYTHONIOENCODING=utf-8 on Windows.
"""
import sys, asyncio, os
from playwright.async_api import async_playwright

BASE, OUT = sys.argv[1].rstrip('/'), sys.argv[2]
os.makedirs(OUT, exist_ok=True)
GPU = ['--use-angle=d3d11', '--enable-gpu', '--ignore-gpu-blocklist']
LOCAL = 'localhost' in BASE
INIT = "localStorage.setItem('spatia_terms_v1','true');localStorage.setItem('spatia_welcomed_v1','true');"
CASES = [
    ('deforestacion', '/?t=misiones&lens=ambiente&a=deforestation_dynamics&dept=Guaraní'),
    ('pm25', '/?t=misiones&lens=ambiente&a=pm25_drivers&dept=Guaraní'),
    ('censo_temporal', '/?t=misiones&lens=poblacion&a=censo_temporal&dept=Capital'),
]


async def cors(route):
    try:
        resp = await route.fetch()
        h = dict(resp.headers)
        h['access-control-allow-origin'] = '*'
        h['access-control-expose-headers'] = 'Content-Length, Content-Range, ETag, Accept-Ranges'
        await route.fulfill(response=resp, headers=h)
    except Exception:
        await route.continue_()


async def main():
    fails = 0
    async with async_playwright() as p:
        b = await p.chromium.launch(channel='chrome', args=GPU)
        ctx = await b.new_context(viewport={'width': 1440, 'height': 1000})
        if LOCAL:
            await ctx.route('**/cdn.spatia.ar/**', cors)
            await ctx.route('**/*.r2.dev/**', cors)
        await ctx.add_init_script(INIT)
        pg = await ctx.new_page()
        errs = []
        pg.on('pageerror', lambda e: errs.append(str(e)[:200]))
        for name, path in CASES:
            await pg.goto(BASE + path, wait_until='domcontentloaded', timeout=180000)
            fr = pg.locator('.chart-frame', has=pg.locator('.chart-frame-title', has_text='Evolución')).first
            try:
                await fr.wait_for(timeout=120000)
                await pg.wait_for_timeout(2000)
                flows = fr.locator('svg path[stroke-width]')   # the flows (header icons are paths too)
                try:
                    await flows.first.wait_for(state='attached', timeout=30000)
                except Exception:   # folded: unfold it (clicking an open frame would fold it)
                    await fr.locator('.chart-frame-header').click()
                    await flows.first.wait_for(state='attached', timeout=60000)
            except Exception as e:
                print(f'{name}: sin gráfico Evolución ({str(e).splitlines()[0][:120]})  FAIL'); fails += 1; continue
            await pg.wait_for_timeout(1500)
            # widest flow = the one with the largest bounding box
            paths = fr.locator('svg path[stroke-width]')
            n = await paths.count()
            best, area = None, -1
            for i in range(n):
                bb = await paths.nth(i).bounding_box()
                if bb and bb['width'] * bb['height'] > area:
                    best, area = i, bb['width'] * bb['height']
            await paths.nth(best).scroll_into_view_if_needed()
            await paths.nth(best).click(force=True)
            await pg.wait_for_timeout(1200)
            bar = pg.locator('.csb').first
            visible = await bar.count() and await bar.is_visible()
            bb = await bar.bounding_box() if visible else None
            inside = bool(bb) and bb['y'] >= 0 and bb['y'] + bb['height'] <= 1000
            text = (await bar.inner_text()).replace('\n', ' ') if visible else '(sin barra)'
            await pg.screenshot(path=f'{OUT}/evolucion_{name}.png')
            ok = visible and inside
            if ok:
                await pg.locator('.csb-save').click()
                await pg.wait_for_timeout(1200)
            zones = await pg.locator('.hzc-chip').evaluate_all('cs => cs.map(c => c.innerText.replace(/\s+/g, " "))') if ok else []
            ok = ok and len(zones) > 0
            print(f"{name}: barra={text} | y={bb['y'] if bb else None} | zonas={zones}  {'OK' if ok else 'FAIL'}")
            fails += 0 if ok else 1
        await b.close()
    print('errores de página:', errs or 'ninguno')
    sys.exit(1 if fails else 0)


asyncio.run(main())
