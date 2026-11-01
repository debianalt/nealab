"""End-to-end check of zones saved from chart selections (F1, 1-nov-2026).

Usage:
    python scripts/verify_zones.py BASE_URL OUT_DIR [QUERY]
    python scripts/verify_zones.py https://www.spatia.ar out/
    python scripts/verify_zones.py http://localhost:4174 out/      # after `npx vite preview --port 4174`

Saves zones from the histogram, Moran, a bivariate quadrant and a distance band,
then a lasso; prints each zone's origin, the CSV of zone B (rows, zone/origin
columns, no "NaN"), the mailto and page errors. Copies zone B's CSV to OUT_DIR
(compare its mean with the panel: they must match, blanks ignored).

Notes (learned the hard way):
- Localhost is not in the R2 CORS list: CDN requests are proxied here with CORS
  added (ctx.route), so a local `vite preview` build can be tested before deploy.
- After a rebuild, restart the preview: the old server keeps serving the old HTML
  (missing chunk hashes). Stopping `npx` leaves node on the port: kill it by PID.
- System Chrome with real GPU flags; run with PYTHONIOENCODING=utf-8 on Windows.
"""
import sys, asyncio, csv, io, json
from urllib.parse import unquote
from playwright.async_api import async_playwright

BASE, OUT = sys.argv[1], sys.argv[2]
QS = sys.argv[3] if len(sys.argv) > 3 else '?t=misiones&lens=poblacion&a=service_deprivation&dept=Capital'
INIT = "localStorage.setItem('spatia_terms_v1','true');localStorage.setItem('spatia_welcomed_v1','true');"
GPU = ['--use-angle=d3d11', '--enable-gpu', '--ignore-gpu-blocklist']
LOCAL = 'localhost' in BASE


async def cors(route):
    try:
        resp = await route.fetch()
        h = dict(resp.headers)
        h['access-control-allow-origin'] = '*'
        h['access-control-expose-headers'] = 'Content-Length, Content-Range, ETag, Accept-Ranges'
        await route.fulfill(response=resp, headers=h)
    except Exception:
        await route.continue_()


def frame(pg, title):
    return pg.locator('.chart-frame', has=pg.locator('.chart-frame-title', has_text=title)).first


async def bar(pg):
    b = pg.locator('.csb').first
    return (await b.inner_text()).replace('\n', ' ') if await b.count() else '(sin barra)'


async def save(pg):
    await pg.locator('.csb-save').click()
    await pg.wait_for_timeout(1500)


async def drag(pg, loc, f0=(0.25, 0.25), f1=(0.75, 0.75)):
    await loc.scroll_into_view_if_needed()
    bb = await loc.bounding_box()
    x0, y0 = bb['x'] + bb['width'] * f0[0], bb['y'] + bb['height'] * f0[1]
    x1, y1 = bb['x'] + bb['width'] * f1[0], bb['y'] + bb['height'] * f1[1]
    await pg.mouse.move(x0, y0); await pg.mouse.down()
    for i in range(1, 9):
        await pg.mouse.move(x0 + (x1 - x0) * i / 8, y0 + (y1 - y0) * i / 8)
    await pg.mouse.up()
    await pg.wait_for_timeout(800)


async def main():
    async with async_playwright() as p:
        b = await p.chromium.launch(channel='chrome', args=GPU)
        ctx = await b.new_context(viewport={'width': 1440, 'height': 1000}, accept_downloads=True)
        if LOCAL:
            await ctx.route('**/cdn.spatia.ar/**', cors)
            await ctx.route('**/*.r2.dev/**', cors)
        pg = await ctx.new_page()
        await pg.add_init_script(INIT)
        errs = []
        pg.on('pageerror', lambda e: errs.append(str(e)[:200]))
        await pg.goto(f'{BASE}/{QS}', wait_until='networkidle', timeout=180000)
        await frame(pg, 'Distribución').locator('svg').first.wait_for(timeout=120000)
        await pg.wait_for_timeout(3000)

        # 1. Histogram brush → zone A
        await drag(pg, frame(pg, 'Distribución').locator('svg').first, (0.35, 0.5), (0.7, 0.5))
        print('1 histograma | barra:', await bar(pg))
        await save(pg)
        print('  tras guardar:', await bar(pg))

        # 2. Moran box → zone B
        ov = pg.locator('.brush-overlay').first
        await drag(pg, ov, (0.55, 0.1), (0.95, 0.45))
        print('2 moran | barra:', await bar(pg))
        await save(pg)

        # 3. Bivariate quadrant → zone C (first Y option)
        bv = frame(pg, 'Bivariado')
        sel = bv.locator('select.picker-select')
        if not await sel.is_visible():
            await bv.locator('.chart-frame-header').click(); await pg.wait_for_timeout(600)
        opts = await sel.locator('option').evaluate_all('os => os.map(o => o.value).filter(Boolean)')
        await sel.select_option(opts[0])
        await pg.wait_for_selector('.bv-quad', timeout=60000)
        await pg.wait_for_timeout(1500)
        await bv.locator('.bv-quad').first.click()
        print('3 bivariado (Y=%s) | barra:' % opts[0], await bar(pg))
        await save(pg)

        # 4. Distance band → zone D
        dp = frame(pg, 'Perfil por distancia')
        if await dp.count():
            await pg.wait_for_selector('.dp-band', timeout=60000)
            await dp.locator('.dp-band').first.click()
            print('4 perfil | barra:', await bar(pg))
            await save(pg)

        # Zones block
        await pg.locator('.hzc').first.scroll_into_view_if_needed()
        chips = await pg.locator('.hzc-chip').evaluate_all('cs => cs.map(c => c.innerText.replace(/\\s+/g, " "))')
        print('zonas:', chips)
        print('gráficos visibles debajo:', await frame(pg, 'Distribución').is_visible())
        blocks = await pg.locator('.zone-block').evaluate_all('bs => bs.map(b => b.innerText.replace(/\\s+/g, " ").slice(0, 200))')
        for bl in blocks: print('  ', bl)
        polys = await pg.locator('.zone-block .hzc-dl-btn', has_text='poly').count()
        print('botones poly (deben ser 0 en zonas de gráfico):', polys)

        # CSV of zone B (Moran)
        async with pg.expect_download() as d:
            await pg.locator('.zone-block').nth(1).locator('.hzc-dl-btn', has_text='csv').click()
        path = await (await d.value).path()
        rows = list(csv.DictReader(io.open(path, encoding='utf-8')))
        cols = list(rows[0].keys()) if rows else []
        print('CSV zona B: filas', len(rows), '| columnas', cols[:8], '...')
        nanish = sum(1 for r in rows for v in r.values() if v in ('NaN', 'undefined'))
        print('  celdas "NaN"/"undefined":', nanish)
        import shutil; shutil.copy(path, f'{OUT}/zoneB.csv')
        shown = await pg.locator('.zone-block').nth(1).locator('.cd-row').first.inner_text()
        print('  panel zona B, 1a fila:', ' '.join(shown.split()))

        href = await pg.locator('.hzc .rd-btn').get_attribute('href')  # RequestDiagnostic
        print('mailto:', unquote(href)[:600])
        # Lasso still makes a zone with its polygon
        await pg.get_by_role('button', name='Lazo').first.click()
        await pg.wait_for_timeout(500)
        bb = await pg.locator('.maplibregl-canvas').first.bounding_box()
        pts = [(0.38, 0.30), (0.50, 0.30), (0.50, 0.42), (0.38, 0.42), (0.38, 0.31)]
        X = lambda f: (bb['x'] + bb['width'] * f[0], bb['y'] + bb['height'] * f[1])
        await pg.mouse.move(*X(pts[0])); await pg.mouse.down()
        for a, c in zip(pts, pts[1:]):
            for i in range(1, 7):
                await pg.mouse.move(*X((a[0] + (c[0] - a[0]) * i / 6, a[1] + (c[1] - a[1]) * i / 6)))
        await pg.mouse.up()
        await pg.wait_for_timeout(2500)
        chips = await pg.locator('.hzc-chip').evaluate_all('cs => cs.map(c => c.innerText.replace(/\\s+/g, " "))')
        print('tras lazo, zonas:', chips)
        last = pg.locator('.zone-block').last
        print('  última zona poly:', await last.locator('.hzc-dl-btn', has_text='poly').count())
        print('  última zona texto:', ' '.join((await last.inner_text()).split())[:300])
        await pg.locator('.hzc').first.screenshot(path=f'{OUT}/zones_block.png')
        await pg.screenshot(path=f'{OUT}/zones_page.png')
        print('errores de página:', errs[:5])
        await b.close()

asyncio.run(main())
