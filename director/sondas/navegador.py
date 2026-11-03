"""Navegador común de las sondas: Chrome del sistema con GPU en Windows,
Chromium de Playwright con SwiftShader en Linux (runner de GitHub)."""
import os, sys

TERMINOS = "spatia_terms_v2"
INIT = (f"localStorage.setItem('{TERMINOS}','true');"
        "localStorage.setItem('spatia_welcomed_v1','true');"
        "localStorage.setItem('spatia_sonda','1');")
VIEWPORT = {'width': 1440, 'height': 900}
# Sin 'HeadlessChrome': la verificación de navegador de Cloudflare desafía ese user-agent.
UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'


async def abrir_navegador(p):
    if sys.platform == 'win32':
        return await p.chromium.launch(channel='chrome', args=['--use-angle=d3d11', '--enable-gpu', '--ignore-gpu-blocklist'])
    return await p.chromium.launch(args=['--use-angle=swiftshader', '--enable-unsafe-swiftshader',
                                         '--ignore-gpu-blocklist', '--enable-webgl'])


async def _cors(route):
    try:
        r = await route.fetch()
        h = dict(r.headers)
        h['access-control-allow-origin'] = '*'
        h['access-control-expose-headers'] = 'Content-Length, Content-Range, ETag, Accept-Ranges'
        await route.fulfill(response=r, headers=h)
    except Exception:
        await route.continue_()


async def contexto_spatia(br, base, locale=None):
    """Contexto con modales salteados, gancho de sonda y, en localhost, proxy CORS a R2
    (localhost:4173 no está en cors-rules.json)."""
    ctx = await br.new_context(viewport=VIEWPORT, user_agent=UA, locale='es-AR')
    init = INIT + (f"localStorage.setItem('nealab-locale','{locale}');" if locale else '')
    await ctx.add_init_script(init)
    # Proxy CORS sólo a pedido (SONDA_PROXY=1): en el runner, Cloudflare desafía las descargas
    # del proxy. El build local se sirve en localhost:5173, que ya está en cors-rules.json.
    if os.environ.get('SONDA_PROXY') == '1':
        await ctx.route('**/cdn.spatia.ar/**', _cors)
        await ctx.route('**/*.r2.dev/**', _cors)
    return ctx
