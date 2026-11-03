"""Screenshots of the map in each theme (Claro · Oscuro · Satélite) + hot switch.

Usage:
    python scripts/verify_theme.py BASE_URL OUT_DIR
    python scripts/verify_theme.py http://localhost:4174 out/     # vite preview
    python scripts/verify_theme.py https://www.spatia.ar out/

For each stored theme: start view, a hexagon layer with a department open, 3D
buildings over Posadas, Chaco (another territory) and the ficha of a hexagon.
Then, on the layer view, switches theme with the bottom-left control and checks
that <html data-theme> follows, the layer stays painted and nothing throws.
Also checks there is no flash: data-theme is already set at DOMContentLoaded.

Same CDN proxy as verify_zones.py (localhost is not in the R2 CORS list).
Run with PYTHONIOENCODING=utf-8 on Windows.
"""
import sys, asyncio, os
from playwright.async_api import async_playwright

BASE, OUT = sys.argv[1].rstrip('/'), sys.argv[2]
os.makedirs(OUT, exist_ok=True)
GPU = ['--use-angle=d3d11', '--enable-gpu', '--ignore-gpu-blocklist']
LOCAL = 'localhost' in BASE
BASE_INIT = "localStorage.setItem('spatia_terms_v2','true');localStorage.setItem('spatia_welcomed_v1','true');"

VIEWS = [
    ('inicio', '/'),
    ('capa_censal', '/?t=misiones&lens=poblacion&a=service_deprivation&dept=Capital'),
    ('inundacion', '/?t=misiones&lens=ambiente&a=flood_risk&dept=Capital'),
    ('chaco', '/?t=chaco'),
    ('ficha', '/?h3=89a946191b3ffff'),
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


async def new_ctx(b, fondo):
    ctx = await b.new_context(viewport={'width': 1440, 'height': 900})
    if LOCAL:
        await ctx.route('**/cdn.spatia.ar/**', cors)
        await ctx.route('**/*.r2.dev/**', cors)
    init = BASE_INIT + (f"localStorage.setItem('spatia_fondo','{fondo}');" if fondo else "localStorage.removeItem('spatia_fondo');")
    await ctx.add_init_script(init)
    return ctx


async def settle(pg, ms=9000):
    await pg.wait_for_timeout(ms)


async def main():
    errs = []
    async with async_playwright() as p:
        b = await p.chromium.launch(channel='chrome', args=GPU)
        for fondo in ['oscuro', 'claro']:
            ctx = await new_ctx(b, fondo)
            pg = await ctx.new_page()
            pg.on('pageerror', lambda e, f=fondo: errs.append(f'{f}: {str(e)[:200]}'))
            pg.on('console', lambda m, f=fondo: errs.append(f'{f} console: {m.text[:200]}') if m.type == 'error' and 'MAP' in m.text else None)
            for name, path in VIEWS:
                await pg.goto(BASE + path, wait_until='domcontentloaded', timeout=180000)
                theme_dcl = await pg.evaluate("document.documentElement.dataset.theme")
                await settle(pg, 12000 if name != 'inicio' else 8000)
                await pg.screenshot(path=f'{OUT}/{fondo}_{name}.png')
                print(f'{fondo:7s} {name:12s} data-theme at DOMContentLoaded = {theme_dcl}')
            # 3D buildings over Posadas
            await pg.goto(BASE + '/?t=misiones', wait_until='domcontentloaded')
            await settle(pg, 6000)
            await pg.evaluate("""() => { const m = window.__spatiaMap; if (m) m.jumpTo({ center: [-55.895, -27.367], zoom: 15.3, pitch: 60, bearing: -20 }); }""")
            await settle(pg, 8000)
            await pg.screenshot(path=f'{OUT}/{fondo}_edificios_3d.png')
            await ctx.close()

        # Hot switch on a layer view: oscuro → claro → satélite → oscuro
        ctx = await new_ctx(b, 'oscuro')
        pg = await ctx.new_page()
        pg.on('pageerror', lambda e: errs.append(f'switch: {str(e)[:200]}'))
        await pg.goto(BASE + VIEWS[1][1], wait_until='domcontentloaded', timeout=180000)
        await settle(pg, 12000)
        for label, expect in [('Claro', 'claro'), ('Satélite', 'oscuro'), ('Oscuro', 'oscuro'), ('Claro', 'claro')]:
            await pg.get_by_role('button', name=label, exact=True).click()
            await settle(pg, 4000)
            got = await pg.evaluate("[document.documentElement.dataset.theme, document.documentElement.dataset.fondo, localStorage.getItem('spatia_fondo')]")
            # the open department must survive a theme switch (a tracked theme read in a
            # territory $effect closed it, found in prod 1-nov-2026)
            ok = 'OK' if got[0] == expect and 'dept=Capital' in pg.url else f'FAIL (url {pg.url})'
            print(f'switch {label:9s} → data-theme={got[0]} fondo={got[1]} stored={got[2]}  {ok}')
            await pg.screenshot(path=f'{OUT}/switch_{label.lower()}_{got[1]}.png')
        await ctx.close()
        await b.close()
    print('errors:', errs or 'none')


asyncio.run(main())
