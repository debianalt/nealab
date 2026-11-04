"""Navegador común de las sondas: Chrome del sistema con GPU en Windows,
Chromium de Playwright con SwiftShader en Linux (runner de GitHub)."""
import os, sys

TERMINOS = "spatia_terms_v2"
INIT = (f"localStorage.setItem('{TERMINOS}','true');"
        "localStorage.setItem('spatia_welcomed_v1','true');"
        "localStorage.setItem('spatia_sonda','1');")
VIEWPORT = {'width': 1280, 'height': 800}
VIEWPORT_MOVIL = {'width': 390, 'height': 844}
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


R2_DEV = 'https://pub-580c676bec7f4eeb96d7d30559a3cab7.r2.dev'


async def _a_r2dev(route):
    # Mismo bucket y mismas reglas CORS que cdn.spatia.ar, pero fuera de la zona de
    # Cloudflare que desafía a los runners de GitHub ("Verifique que es un ser humano").
    url = route.request.url
    await route.continue_(url=R2_DEV + url[url.index('cdn.spatia.ar') + len('cdn.spatia.ar'):])


UA_MOVIL = 'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36'


async def contexto_spatia(br, base, locale=None, fondo=None, movil=False):
    """Contexto con modales salteados y gancho de sonda; en localhost, datos desde r2.dev.
    fondo: 'claro' | 'oscuro' | 'satelite' (localStorage spatia_fondo). movil: 390×844 táctil."""
    if movil:
        ctx = await br.new_context(viewport=VIEWPORT_MOVIL, user_agent=UA_MOVIL, locale='es-AR', is_mobile=True, has_touch=True, device_scale_factor=2)
    else:
        ctx = await br.new_context(viewport=VIEWPORT, user_agent=UA, locale='es-AR')
    init = INIT + (f"localStorage.setItem('nealab-locale','{locale}');" if locale else '')
    if fondo:
        init += f"localStorage.setItem('spatia_fondo','{fondo}');"
    await ctx.add_init_script(init)
    # Proxy CORS sólo a pedido (SONDA_PROXY=1): en el runner, Cloudflare desafía las descargas
    # del proxy. El build local se sirve en localhost:5173, que ya está en cors-rules.json.
    if 'localhost' in base or os.environ.get('SONDA_R2DEV') == '1':
        await ctx.route('**/cdn.spatia.ar/**', _a_r2dev)
    if os.environ.get('SONDA_PROXY') == '1':
        await ctx.route('**/cdn.spatia.ar/**', _cors)
        await ctx.route('**/*.r2.dev/**', _cors)
    return ctx
