"""Sonda del mapa: recorre territorio × capa × pestaña y compara lo que se ve.

Uso:
  python director/sondas/mapa.py BASE OUT [--territorios a,b] [--capas x,y] [--sin-depto] [--max-capas N]

Por cada (territorio, capa):
  - vista de provincia: leyenda, relleno por departamento, ranking, cada pestaña;
  - vista de departamento (el primero del ranking): hexágonos pintados contra leyenda,
    tooltip contra el valor del hexágono, gráficos, cada pestaña.
Escribe OUT/hallazgos_mapa.json (lista) y OUT/vistas_mapa.json (qué se recorrió).
"""
import argparse, asyncio, json, os, re, sys, time
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from navegador import abrir_navegador, contexto_spatia
from comun import Hallazgos, reglas, revisar_texto, numeros, RUIDO_CONSOLA

ESTADO_JS = (AQUI / 'estado.js').read_text(encoding='utf-8')
LENTE_POR_DEFECTO = {'ambiente', 'produccion', 'poblacion', 'economia'}


async def estado(page, muestra_hover=0):
    return await page.evaluate(ESTADO_JS, {'muestraHover': muestra_hover})


async def esperar_lista(page, analisis, timeout=120, dept=None, con_hex=False):
    """Espera a que la capa esté cargada (y, si se pide, el departamento con hexágonos)."""
    t0 = time.time(); e = None
    while time.time() - t0 < timeout:
        try:
            e = await estado(page)
        except Exception:
            await page.wait_for_timeout(1000); continue
        listo = (not e.get('sinGancho') and e.get('analisis') == analisis and not e.get('cargando')
                 and e.get('leyenda') is not None)
        if dept: listo = listo and e.get('deptSel') == dept
        if con_hex: listo = listo and (e.get('hex') or {}).get('n', 0) > 0
        if listo:
            await page.wait_for_timeout(1500)
            return await estado(page), time.time() - t0
        await page.wait_for_timeout(1000)
    return e, None


async def mapa_quieto(page, ms=20000):
    await page.evaluate("""(ms) => new Promise(r => { const m = window.__spatia?.map; if (!m) return r();
        const t = setTimeout(r, ms); m.once('idle', () => { clearTimeout(t); r(); }); m.triggerRepaint(); })""", ms)


def matriz(reg, territorios_ok, filtro_t=None, filtro_c=None):
    """(territorio, lente, capa) disponibles según ANALYSIS_REGISTRY: Misiones falta en los
    mapas coverage y cuenta como disponible (AnalysisMenu.svelte)."""
    out = []
    for a in reg['ANALYSIS_REGISTRY']:
        if a.get('status') not in (None, 'available'): continue
        for t in territorios_ok:
            cov = (a.get('coverage') or {}).get(t, 'available' if t == 'misiones' else 'pending')
            if cov != 'available': continue
            if filtro_t and t not in filtro_t: continue
            if filtro_c and a['id'] not in filtro_c: continue
            out.append((t, a['lensId'], a['id']))
    return out


def rangos_leyenda(items):
    """Números de las etiquetas de la leyenda ('0,0 – 3,0' → (0, 3))."""
    out = []
    for it in items:
        ns = numeros(it['label'])
        if len(ns) >= 2: out.append((ns[0], ns[1]))
        elif len(ns) == 1: out.append((ns[0], ns[0]))
    return out


def revisar_provincia(H, e, ctx):
    lg = e.get('leyenda')
    if not lg or not (lg['items'] or lg.get('gradiente')):
        H.add('leyenda_ausente', 'alta', ctx, 'La capa está activa pero no hay leyenda visible.')
        return
    df = e.get('deptFill')
    if df and df.get('valores'):
        vals = [x['value'] for x in df['valores'] if x.get('value') is not None]
        r = rangos_leyenda(lg['items'])
        if vals and r:
            lo, hi = min(a for a, _ in r), max(b for _, b in r)
            tol = max(abs(hi), 1) * 0.02 + 0.051
            fuera = [x for x in df['valores'] if x.get('value') is not None and not (lo - tol <= abs(x['value']) <= hi + tol)]
            if fuera:
                H.add('relleno_fuera_de_leyenda', 'media', ctx,
                      f'{len(fuera)} departamentos con valor fuera del rango de la leyenda ({lo}–{hi}): '
                      + ', '.join(f"{x['name']}={x['value']}" for x in fuera[:5]))
        # Ranking ↔ relleno: el número de cada fila del ranking es el que pinta el mapa
        porNombre = {x['name']: x['value'] for x in df['valores']}
        malos = []
        for row in e.get('ranking') or []:
            if row['nombre'] not in porNombre or porNombre[row['nombre']] is None: continue
            ns = numeros(row['valor'] or '')
            if not ns: continue
            esperado = porNombre[row['nombre']]
            dec = df.get('decimales') or 0
            if min(abs(abs(n) - abs(esperado)) for n in ns) > 0.5 * 10 ** (-dec) + 1e-6 + abs(esperado) * 0.005:
                malos.append(f"{row['nombre']}: ranking «{row['valor']}» vs mapa {esperado}")
        if malos:
            H.add('ranking_vs_mapa', 'alta', ctx, f'{len(malos)} filas del ranking no coinciden con el relleno del mapa: ' + '; '.join(malos[:4]))


def revisar_hex(H, e, ctx):
    hx, lg = e.get('hex'), e.get('leyenda')
    if not hx: return
    if not lg:
        H.add('leyenda_ausente', 'alta', ctx, 'Hay hexágonos pintados pero no hay leyenda.'); return
    if hx['ceroComoNodata']:
        H.add('cero_como_sin_dato', 'media', ctx,
              f"{hx['ceroComoNodata']} de {hx['cero']} hexágonos con valor 0 pintados con el color de «sin dato» (regla: 0 real ≠ sin cobertura).")
    if e.get('clases') and lg['items']:
        colores_ley = [it['color'] for it in lg['items']]
        total = max(1, hx['n'] - hx['nodata'])
        raros = [c for c in hx['colores'] if c['c'] not in colores_ley and c['c'] != hx.get('nodataColor') and c['n'] > total * 0.01]
        if raros:
            H.add('colores_fuera_de_leyenda', 'alta', ctx,
                  f"{sum(c['n'] for c in raros)} hexágonos pintados con colores que la leyenda no muestra: "
                  + ', '.join(f"rgb({c['c']}) n={c['n']} valores {c['min']:.2f}–{c['max']:.2f}" for c in raros[:4]))
        # Cada color de la leyenda cubre el rango de su clase
        cl = e['clases']; steps = cl['steps']; off = 1 if cl.get('zero') else 0
        por_codigo = cl.get('ordinal') or cl.get('nominal')
        for c in hx['colores']:
            if c['c'] not in colores_ley: continue
            k = colores_ley.index(c['c']) - off
            if k < 0 or k >= len(steps): continue
            if por_codigo:
                # clases por código: un color, un código (la vista agregada usa la moda)
                if abs(c['min'] - steps[k]['from']) > 1e-6 or abs(c['max'] - steps[k]['from']) > 1e-6:
                    H.add('clase_incoherente', 'alta', ctx,
                          f"Color de «{lg['items'][k + off]['label']}» (código {steps[k]['from']}) pinta códigos {c['min']:g}–{c['max']:g}.")
                continue
            lo = steps[k]['from']; hi = steps[k + 1]['from'] if k + 1 < len(steps) else float('inf')
            if cl.get('upperInclusive'):
                fuera = (k > 0 and c['min'] <= lo + 1e-9) or c['max'] > hi + 1e-6  # clase k = (lo, hi]
            else:
                fuera = c['min'] < lo - 1e-6 or c['max'] >= hi + 1e-6
            if fuera:
                H.add('clase_incoherente', 'alta', ctx,
                      f"Color de «{lg['items'][k + off]['label']}» (desde {lo}) pinta valores {c['min']:.2f}–{c['max']:.2f}.")
    sp = hx.get('spearman')
    if sp and sp['rho'] is not None and e.get('escalaColor') != 'categorical':
        rho = sp['rho']
        if e.get('escalaColor') == 'diverging' or e.get('modoTemporal') == 'delta': rho = abs(rho)
        if rho < 0.85:
            H.add('rampa_no_monotona', 'alta', ctx, f'El color no sigue al valor según la rampa de la leyenda (Spearman {sp["rho"]:.2f}, n={sp["n"]}).')
        if sp['fueraDeRampa'] > sp['n'] * 0.1:
            H.add('colores_fuera_de_rampa', 'media', ctx, f"{sp['fueraDeRampa']} de {sp['n']} hexágonos con colores que no están en la rampa de la leyenda.")


async def revisar_tooltip(H, page, e, ctx):
    v = e.get('variable') or {}
    cl = e.get('clases') or {}
    etiquetas = {st['from']: st['label'] for st in cl.get('steps', [])} if (cl.get('ordinal') or cl.get('nominal')) else None
    malos_clase = []
    escala = v.get('scale') or 1
    unidad = v.get('unit')
    malos_valor, malos_unidad, punto = [], [], 0
    for s in e.get('enPantalla') or []:
        await page.mouse.move(s['x'], s['y']); await page.wait_for_timeout(250)
        tt = await page.evaluate("() => { const t = document.querySelector('#hover-tooltip'); return t && t.style.display !== 'none' ? t.innerText : null }")
        if not tt: continue
        H.revisar(revisar_texto(tt), ctx, 'tooltip')
        if s.get('nodata') in (True, 'true') or s.get('type_label'): continue
        if s['value'] is None: continue
        if etiquetas is not None:
            lab = etiquetas.get(float(s['value'])) or etiquetas.get(int(float(s['value'])))
            if lab and lab not in tt: malos_clase.append(f"código {s['value']} = «{lab}», tooltip «{tt.strip()}»")
            continue
        esperado = float(s['value']) * escala
        if re.search(r'\d\.\d', tt): punto += 1
        ns = numeros(tt)
        if not ns:
            if float(s['value']) == 0 and re.search(r'sin (dato|cobertura)', tt, re.I):
                malos_valor.append(f'valor 0 rotulado «{tt.strip()}»')
            continue
        if min(abs(n - esperado) for n in ns) > 0.051 + abs(esperado) * 0.01:
            malos_valor.append(f'hexágono {float(s["value"]):.4g}×{escala} = {esperado:.2f}, tooltip «{tt.strip()}»')
        if unidad and unidad not in ('/100',) and unidad not in tt:
            malos_unidad.append(f'«{tt.strip()}» sin «{unidad}»')
    await page.mouse.move(5, 5)
    if malos_clase:
        H.add('tooltip_clase', 'alta', ctx, 'El tooltip no muestra el nombre de la clase pintada: ' + '; '.join(malos_clase[:3]))
    if malos_valor:
        H.add('tooltip_valor', 'alta', ctx, f'El tooltip no muestra el valor pintado ({e.get("colorVar") or e.get("primaria")}): ' + '; '.join(malos_valor[:3]))
    if malos_unidad:
        H.add('tooltip_unidad', 'media', ctx, f'El tooltip no lleva la unidad de la variable pintada ({e.get("primaria")}, {unidad}): ' + '; '.join(malos_unidad[:3]))
    if punto:
        H.add('tooltip_decimal', 'baja', {**ctx, 'pestana': None}, 'El tooltip usa punto decimal («12.5») mientras leyenda y panel usan coma («12,5»).')


async def abrir_graficos(page):
    for hdr in await page.query_selector_all('.chart-frame-header'):
        try:
            t = (await hdr.inner_text()).strip()
            if t.startswith('▸'): await hdr.click(); await page.wait_for_timeout(600)
        except Exception: pass


def revisar_graficos(H, e, ctx):
    for g in e.get('graficos') or []:
        c = {**ctx, 'grafico': g['titulo']}
        H.revisar(revisar_texto(g['texto']), c, 'grafico')
        if g['abierto'] and g['marcas'] == 0:
            H.add('grafico_vacio', 'alta', c, f"El gráfico «{g['titulo']}» está abierto pero no dibuja nada.")


def revisar_comun(H, e, ctx):
    if e.get('errorCarga'): H.add('error_carga', 'alta', ctx, f"Error de carga: {e['errorCarga']}")
    for t in e.get('toasts') or []:
        if t: H.add('toast_error', 'alta', ctx, f'Aviso de error en pantalla: «{t[:200]}»')
    H.revisar(revisar_texto(e.get('textoPanel', '')), ctx, 'panel')
    if e.get('leyenda'): H.revisar(revisar_texto(e['leyenda']['texto']), ctx, 'leyenda')


async def tabs(page):
    return await page.query_selector_all('.rank-tab')


async def recorrer(page, base, t, lente, capa, H, vistas, args, errores):
    q = f"/?t={t}&lens={lente}&a={capa}"
    ctx = {'territorio': t, 'capa': capa, 'vista': 'provincia', 'url': base + q, 'pestana': None}
    errores.clear()
    await page.goto(base + q, wait_until='domcontentloaded')
    e, dt = await esperar_lista(page, capa)
    if dt is None:
        H.add('no_carga', 'alta', ctx, f'La capa no terminó de cargar en 120 s (estado: {json.dumps({k: (e or {}).get(k) for k in ("analisis", "cargando", "errorCarga")}, ensure_ascii=False)}).')
        await H.captura(page, ctx); return
    vistas.append({**ctx, 'seg': round(dt, 1)})
    revisar_comun(H, e, ctx); revisar_provincia(H, e, ctx)
    nombres_tabs = e.get('pestanas') or []
    for i, nombre in enumerate(nombres_tabs):
        if nombre == e.get('pestanaActiva'): continue
        tb = (await tabs(page))[i]; await tb.click(); await page.wait_for_timeout(1800)
        e2 = await estado(page); c2 = {**ctx, 'pestana': nombre}
        revisar_comun(H, e2, c2); revisar_provincia(H, e2, c2)
    H.consola(errores, ctx); await H.captura_si(page, ctx)
    # Departamento: el primero del ranking (o del relleno)
    dept = None
    if e.get('ranking'): dept = e['ranking'][0]['nombre']
    elif e.get('deptFill') and e['deptFill']['valores']: dept = e['deptFill']['valores'][0]['name']
    if e.get('unidadEspacial', 'hexagon') != 'hexagon':
        vistas.append({**ctx, 'nota': f"unidad espacial {e['unidadEspacial']}: sin sonda de hexágonos"}); return
    if args.sin_depto or not dept: return
    q2 = q + f"&dept={quote(dept)}"
    ctx = {'territorio': t, 'capa': capa, 'vista': 'departamento', 'depto': dept, 'url': base + q2, 'pestana': None}
    errores.clear()
    await page.goto(base + q2, wait_until='domcontentloaded')
    e, dt = await esperar_lista(page, capa, dept=dept, con_hex=True, timeout=150)
    if dt is None:
        H.add('no_carga', 'alta', ctx, f'El departamento {dept} no mostró hexágonos en 150 s.')
        await H.captura(page, ctx); return
    await mapa_quieto(page)
    e = await estado(page, muestra_hover=args.hover)
    vistas.append({**ctx, 'seg': round(dt, 1), 'hex': e['hex']['n']})
    revisar_comun(H, e, ctx); revisar_hex(H, e, ctx); await revisar_tooltip(H, page, e, ctx)
    await abrir_graficos(page); await page.wait_for_timeout(2500)
    revisar_graficos(H, await estado(page), ctx)
    nombres_tabs = e.get('pestanas') or []
    for i, nombre in enumerate(nombres_tabs):
        if nombre == e.get('pestanaActiva'): continue
        tb = (await tabs(page))[i]; await tb.click(); await page.wait_for_timeout(2500)
        await mapa_quieto(page, 10000)
        e2 = await estado(page, muestra_hover=args.hover); c2 = {**ctx, 'pestana': nombre}
        revisar_comun(H, e2, c2); revisar_hex(H, e2, c2); await revisar_tooltip(H, page, e2, c2)
    H.consola(errores, ctx); await H.captura_si(page, ctx)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('base'); ap.add_argument('out')
    ap.add_argument('--territorios'); ap.add_argument('--capas')
    ap.add_argument('--sin-depto', action='store_true')
    ap.add_argument('--hover', type=int, default=6)
    ap.add_argument('--max-capas', type=int, default=0)
    args = ap.parse_args()
    base = args.base.rstrip('/'); out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    R = reglas()
    H = Hallazgos(out, 'mapa')
    vistas = []
    async with async_playwright() as p:
        br = await abrir_navegador(p)
        cx = await contexto_spatia(br, base)
        page = await cx.new_page()
        errores = []
        page.on('console', lambda m: errores.append(m.text) if m.type == 'error' else None)
        page.on('pageerror', lambda x: errores.append('PAGEERROR ' + str(x)))
        page.on('response', lambda r: errores.append(f'HTTP {r.status} {r.url}') if r.status >= 400 else None)
        await page.goto(base + '/', wait_until='domcontentloaded')
        await page.wait_for_function('() => window.__spatia && window.__spatia.registros', timeout=90000)
        reg = await page.evaluate("""() => { const r = window.__spatia.registros; return {
            ANALYSIS_REGISTRY: r.ANALYSIS_REGISTRY.map(a => ({ id: a.id, lensId: a.lensId, coverage: a.coverage, status: a.status })) } }""")
        M = matriz(reg, R['territorios_analisis'],
                   set(args.territorios.split(',')) if args.territorios else None,
                   set(args.capas.split(',')) if args.capas else None)
        if args.max_capas: M = M[:args.max_capas]
        print(f'{len(M)} pares territorio × capa', flush=True)
        for i, (t, lente, capa) in enumerate(M, 1):
            t0 = time.time()
            try:
                await recorrer(page, base, t, lente, capa, H, vistas, args, errores)
            except Exception as ex:
                H.add('sonda_fallo', 'media', {'territorio': t, 'capa': capa, 'vista': '?', 'url': page.url},
                      f'La sonda falló en esta vista: {type(ex).__name__}: {str(ex)[:300]}')
                try: page = await cx.new_page()
                except Exception: pass
            print(f'[{i}/{len(M)}] {t} {capa}: {time.time() - t0:.0f} s, {H.total()} hallazgos', flush=True)
        await br.close()
    H.guardar()
    (out / 'vistas_mapa.json').write_text(json.dumps(vistas, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    asyncio.run(main())
