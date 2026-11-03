"""Sonda estática: config y registros, colores en textos, esquemas en R2, fallos de CI.

Uso: python director/sondas/estatico.py BASE OUT
Los registros se leen de la página (gancho __spatia), igual que las otras sondas.
"""
import argparse, asyncio, json, os, subprocess, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from playwright.async_api import async_playwright

AQUI = Path(__file__).parent
sys.path.insert(0, str(AQUI))
from navegador import abrir_navegador, contexto_spatia
from comun import Hallazgos, reglas, RAIZ


def correr(cmd, timeout=900):
    r = subprocess.run(cmd, cwd=RAIZ, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=timeout,
                       env={**os.environ, 'PYTHONIOENCODING': 'utf-8'})
    return r.returncode, r.stdout + r.stderr


async def registros(base):
    async with async_playwright() as p:
        br = await abrir_navegador(p)
        cx = await contexto_spatia(br, base); page = await cx.new_page()
        await page.goto(base + '/', wait_until='domcontentloaded')
        await page.wait_for_function('() => window.__spatia && window.__spatia.registros', timeout=90000)
        reg = await page.evaluate("""() => { const r = window.__spatia.registros, t = window.__spatia.i18n;
          return {
            analisis: r.ANALYSIS_REGISTRY.map(a => ({ id: a.id, lens: a.lensId, coverage: a.coverage, comparable: a.comparable, status: a.status })),
            capas: Object.values(r.HEX_LAYER_REGISTRY).map(l => ({ id: l.id, coverage: l.coverage, primary: l.primaryVariable,
              variables: (l.variables || []).map(v => ({ col: v.col, unit: v.unit ?? null, hidden: !!v.hidden, label: t.t(v.labelKey), labelKey: v.labelKey })),
              titulo: t.t(l.titleKey), titleKey: l.titleKey })),
            territorios: Object.keys(r.TERRITORY_REGISTRY) } }""")
        await br.close()
        return reg


def revisar_registros(H, reg, R):
    ok = set(R['territorios_analisis'])
    hex_por_id = {c['id']: c for c in reg['capas']}
    for a in reg['analisis']:
        cov = a.get('coverage') or {}
        fuera = [t for t, v in cov.items() if v == 'available' and t not in ok]
        if fuera:
            H.add('alcance', 'alta', {'capa': a['id'], 'vista': 'config'},
                  f"La capa {a['id']} figura disponible fuera del alcance (NEA AR + Itapúa + Alto Paraná): {', '.join(fuera)}", clave=','.join(sorted(fuera)))
        hx = hex_por_id.get(a['id'])
        if hx and hx.get('coverage'):
            dif = sorted(t for t in set(cov) | set(hx['coverage']) if cov.get(t) != hx['coverage'].get(t))
            if dif:
                H.add('deriva_coverage', 'media', {'capa': a['id'], 'vista': 'config'},
                      f"coverage distinto en ANALYSIS_REGISTRY y HEX_LAYER_REGISTRY para {a['id']}: {', '.join(dif[:8])}", clave=','.join(dif))
    for c in reg['capas']:
        if c['titulo'] == c['titleKey']:
            H.add('clave_i18n', 'media', {'capa': c['id'], 'vista': 'config'}, f"El título de {c['id']} es la clave sin traducir «{c['titleKey']}».", clave=c['titleKey'])
        for v in c['variables']:
            if v['hidden'] or 'type' in v['col']: continue  # tipos categóricos: sin unidad
            if not v['unit']:
                H.add('unidad_faltante', 'media', {'capa': c['id'], 'vista': 'config'}, f"La variable {v['col']} ({v['label']}) no declara unidad.", clave=v['col'])
            if v['label'] == v['labelKey']:
                H.add('clave_i18n', 'media', {'capa': c['id'], 'vista': 'config'}, f"La etiqueta de {v['col']} es la clave sin traducir «{v['labelKey']}».", clave=v['labelKey'])


def colores_en_textos(H):
    code, out = correr([sys.executable, 'scripts/check_color_words.py'])
    if code:
        for linea in out.splitlines():
            if linea.strip() and not linea.startswith('ok'):
                H.add('color_en_texto', 'baja', {'vista': 'codigo'}, f'Texto de lectura nombra un color: {linea.strip()[:240]}', clave=linea.strip()[:80])


def esquemas(H):
    code, out = correr([sys.executable, 'pipeline/audit_parquet_schemas.py'], timeout=1200)
    ruta = RAIZ / 'pipeline/output/schema_audit.json'
    if not ruta.exists():
        H.add('sonda_fallo', 'baja', {'vista': 'esquemas'}, f'audit_parquet_schemas.py no dejó informe: {out[-300:]}'); return
    try:
        rep = json.loads(ruta.read_text(encoding='utf-8'))
    except Exception:
        return
    items = rep if isinstance(rep, list) else rep.get('results') or rep.get('layers') or []
    for it in items:
        if not isinstance(it, dict): continue
        nombre = it.get('analysis') or it.get('parquet') or '?'
        if it.get('status') == 'unreachable':
            H.add('parquet_inaccesible', 'alta', {'capa': str(nombre), 'vista': 'datos'}, f"No se pudo leer {it.get('parquet')}", clave=str(it.get('parquet')).split('?')[0])
        if it.get('missing'):
            H.add('esquema_parquet', 'alta', {'capa': str(nombre), 'vista': 'datos'},
                  f"Columnas que config espera y el parquet en R2 no tiene ({it.get('parquet')}): {it['missing'][:10]}", clave=','.join(it['missing'][:10]))


def fallos_ci(H, dias=16):
    if not os.environ.get('GH_TOKEN') and not os.environ.get('GITHUB_TOKEN'):
        code, _ = correr(['gh', 'auth', 'status'])
        if code: return
    desde = (datetime.now(timezone.utc) - timedelta(days=dias)).strftime('%Y-%m-%d')
    code, out = correr(['gh', 'run', 'list', '--limit', '100', '--created', f'>={desde}',
                        '--json', 'workflowName,conclusion,createdAt,url'])
    if code: return
    vistos = set()
    for r in json.loads(out or '[]'):
        if r['workflowName'] == 'Director de Spatia' or r['conclusion'] != 'failure': continue
        if r['workflowName'] in vistos: continue
        vistos.add(r['workflowName'])
        H.add('ci_fallo', 'alta', {'vista': 'ci', 'workflow': r['workflowName']},
              f"El workflow «{r['workflowName']}» falló el {r['createdAt'][:10]}: {r['url']}", clave=r['workflowName'])


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('base'); ap.add_argument('out')
    ap.add_argument('--sin-esquemas', action='store_true')
    args = ap.parse_args()
    H = Hallazgos(Path(args.out), 'estatico'); R = reglas()
    reg = asyncio.run(registros(args.base.rstrip('/')))
    revisar_registros(H, reg, R); print('registros:', H.total(), flush=True)
    colores_en_textos(H); print('colores:', H.total(), flush=True)
    fallos_ci(H); print('ci:', H.total(), flush=True)
    if not args.sin_esquemas:
        esquemas(H); print('esquemas:', H.total(), flush=True)
    H.guardar()


if __name__ == '__main__':
    main()
