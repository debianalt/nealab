"""End-to-end check of the hexagon fact sheet (ficha) with Playwright.

Usage:
    python scripts/verify_ficha.py [BASE_URL] [H3 ...]
    python scripts/verify_ficha.py https://www.spatia.ar 89a946191b3ffff 89a95099b8fffff

Opens each ?h3=<cell> link, waits for the sheet and prints, per layer, the summary
row (value + department percentile). Also opens Capital (flood), taps a hexagon and
uses «Ficha completa» to check the in-app entry point.

Notes (learned the hard way):
- No Playwright browsers are installed: it drives the system Chrome (channel='chrome').
- Use the real GPU flags; headless SwiftShader is far slower and not representative.
- The terms/welcome modal is skipped via localStorage before load.
- Run with PYTHONIOENCODING=utf-8 on Windows (labels carry ², ³, accents).
"""
import sys
import time
import asyncio
from playwright.async_api import async_playwright

BASE = sys.argv[1] if len(sys.argv) > 1 else 'http://localhost:5174'
CELLS = sys.argv[2:] or ['89a946191b3ffff']  # Capital (Misiones)
INIT = "localStorage.setItem('spatia_terms_v1','true');localStorage.setItem('spatia_welcomed_v1','true');"
GPU = ['--use-angle=d3d11', '--enable-gpu', '--ignore-gpu-blocklist']
ROWS = "[...document.querySelectorAll('.fi-layer summary')].map(s => s.innerText.replace(/\\s+/g,' ').trim())"
HEAD = "document.querySelector('.fi-head')?.innerText.replace(/\\s+/g,' ')"


async def open_page(browser, qs):
    pg = await browser.new_page(viewport={'width': 1366, 'height': 768})
    await pg.add_init_script(INIT)
    errs: list[str] = []
    pg.on('pageerror', lambda e: errs.append(str(e)[:160]))
    await pg.goto(f'{BASE}/{qs}', wait_until='networkidle', timeout=120000)
    return pg, errs


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(channel='chrome', args=GPU)

        for cell in CELLS:
            pg, errs = await open_page(browser, f'?h3={cell}')
            t0 = time.time()
            await pg.wait_for_selector('.fi-layer', timeout=60000)
            await pg.wait_for_function("!document.querySelector('.fi-state')", timeout=60000)
            print(f'\n?h3={cell}  ({time.time() - t0:.1f}s after load)  {await pg.evaluate(HEAD)}')
            for row in await pg.evaluate(ROWS):
                print('   ', row)
            print('   page errors:', errs[:3])
            await pg.close()

        # In-app entry point: tap a hexagon in Capital, then «Ficha completa».
        pg, errs = await open_page(browser, '?lens=ambiente&a=flood_risk&dept=Capital')
        await pg.wait_for_timeout(9000)
        await pg.mouse.click(600, 450)
        await pg.wait_for_timeout(3000)
        if await pg.locator('.hc-ficha-btn').count():
            await pg.locator('.hc-ficha-btn').click(force=True)
            await pg.wait_for_selector('.fi-layer', timeout=60000)
            print('\nIn-app button OK:', await pg.evaluate(HEAD), '| layers:', len(await pg.evaluate(ROWS)))
        else:
            print('\nIn-app button: no hexagon under the click (map not loaded or point off-hex)')
        print('   page errors:', errs[:3])
        await pg.close()
        await browser.close()


asyncio.run(main())
