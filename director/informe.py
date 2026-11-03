"""Arma el informe de la corrida y lo publica como issue (cierra el anterior).

Uso: python director/informe.py RUN_DIR [--publicar]
Lee RUN_DIR/{hallazgos.json, triage.json, arreglos.json, despliegue.json} y escribe RUN_DIR/informe.md.
"""
import argparse, json, os, subprocess, sys
from datetime import date
from pathlib import Path


def leer(p, defecto):
    try: return json.loads(Path(p).read_text(encoding='utf-8'))
    except Exception: return defecto


def armar(run):
    run = Path(run)
    H = leer(run / 'hallazgos.json', [])
    T = leer(run / 'triage.json', {})
    A = leer(run / 'arreglos.json', [])
    D = leer(run / 'despliegue.json', {})
    porH = {h['huella']: h for h in H}
    url_run = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    L = [f"**Resumen.** {T.get('resumen') or f'{len(H)} hallazgos de las sondas.'}", '']
    modo = os.environ.get('MODO', 'diagnostico')
    L.append(f"Modo: `{modo}` · modelo: `{os.environ.get('MODELO', '?')}` · [corrida y capturas]({url_run})")
    L.append('')
    if D:
        L += ['## Despliegue', '', D.get('texto', ''), '']
    arr = [a for a in A if a.get('estado') == 'arreglado']
    no = [a for a in A if a.get('estado') != 'arreglado']
    if arr:
        L += ['## Arreglado', '']
        for a in arr:
            L.append(f"- **{a['grupo']}** ({a.get('commit') or 's/commit'}): {a.get('verificacion', '')} — {len(a.get('huellas', []))} hallazgos")
        L.append('')
    if no:
        L += ['## Intentado y no arreglado', '']
        for a in no:
            L.append(f"- **{a['grupo']}**: {a.get('motivo', '')}")
        L.append('')
    if T.get('decidir'):
        L += ['## Preguntas para vos', '', 'Respondé con un comentario en este issue (por ejemplo «1: opción 2»). La corrida siguiente lo registra en `director/decisiones.md`.', '']
        for i, q in enumerate(T['decidir'], 1):
            L.append(f"**{i}. {q['pregunta']}**")
            if q.get('contexto'): L.append(f"{q['contexto']}")
            for j, o in enumerate(q.get('opciones', []), 1): L.append(f"   {j}. {o}")
            L.append('')
    pend = [g for g in T.get('arreglar', []) if g['grupo'] not in {a['grupo'] for a in A}]
    if pend and modo != 'completo':
        L += ['## Para arreglar (se arregla solo en modo completo)', '']
        for g in pend:
            L.append(f"- **{g.get('titulo') or g['grupo']}** — {g.get('diagnostico', '')[:300]} ({len(g.get('huellas', []))} hallazgos)")
        L.append('')
    if T.get('datos'):
        L += ['## Datos: para una sesión local', '']
        for d in T['datos']:
            L.append(f"- **{d['titulo']}**: {d.get('explicacion', '')} Correr: `{d.get('que_correr', '')}`")
        L.append('')
    if T.get('falsos_positivos'):
        L.append(f"<details><summary>{len(T['falsos_positivos'])} falsos positivos descartados</summary>\n")
        for f in T['falsos_positivos']:
            h = porH.get(f['huella'], {})
            L.append(f"- `{f['huella']}` {h.get('tipo', '')} {h.get('capa', '')}: {f.get('motivo', '')}")
        L.append('\n</details>\n')
    L.append(f"<details><summary>Todos los hallazgos ({len(H)})</summary>\n")
    for h in H[:300]:
        donde = ' · '.join(str(h[k]) for k in ('territorio', 'capa', 'vista', 'pestana', 'pagina', 'idioma') if h.get(k))
        L.append(f"- [{h['severidad']}] `{h['tipo']}` {donde}: {h['detalle'][:220]}")
    L.append('\n</details>')
    return '\n'.join(L)


def gh(*a, entrada=None):
    return subprocess.run(['gh', *a], capture_output=True, text=True, encoding='utf-8', input=entrada)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('run'); ap.add_argument('--publicar', action='store_true')
    ap.add_argument('--titulo', default=f'Director · {date.today().isoformat()}')
    a = ap.parse_args()
    cuerpo = armar(a.run)
    (Path(a.run) / 'informe.md').write_text(cuerpo, encoding='utf-8')
    print(cuerpo[:3000])
    if not a.publicar: return
    gh('label', 'create', 'director', '--color', 'a2452a', '--description', 'Informes del director de Spatia')
    previos = json.loads(gh('issue', 'list', '--label', 'director', '--state', 'open', '--json', 'number').stdout or '[]')
    r = gh('issue', 'create', '--title', a.titulo, '--label', 'director', '--body-file', '-', entrada=cuerpo)
    print(r.stdout, r.stderr, file=sys.stderr)
    nuevo = r.stdout.strip()
    for p in previos:
        gh('issue', 'close', str(p['number']), '--comment', f'Reemplazado por {nuevo}. Las respuestas de este issue quedaron en `director/decisiones.md`.')


if __name__ == '__main__':
    main()
