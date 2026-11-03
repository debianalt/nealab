"""Guardia del director: decide por código si un cambio puede ir solo a producción.

Uso: python director/guardia.py BASE_REF        (compara BASE_REF..HEAD + árbol de trabajo)
     python director/guardia.py --diff ARCHIVO   (prueba con un diff unificado)
Sale con 0 si el cambio está permitido; 1 si no, con los motivos.

Reglas (reglamento.md §1): sólo src/ y static/ (más ignorar.yaml y decisiones.md);
nada de crear o eliminar capas o territorios, tocar coverage (alcance), cache-busters ?v,
URLs de datos, dominios externos nuevos ni src/lib/data/; tope de líneas cambiadas.
"""
import re, subprocess, sys
from pathlib import Path
import yaml

RAIZ = Path(__file__).resolve().parents[1]
R = yaml.safe_load((RAIZ / 'director/reglas.yaml').read_text(encoding='utf-8'))

ID_CAPA = re.compile(r"^\s*(?:id\s*:\s*'([a-z0-9_]+)'|([a-z0-9_]+)\s*:\s*\{\s*$)")


def diff_git(base):
    a = subprocess.run(['git', 'diff', '--no-color', '-U0', base], cwd=RAIZ, capture_output=True, text=True, encoding='utf-8').stdout
    b = subprocess.run(['git', 'diff', '--no-color', '-U0', '--cached'], cwd=RAIZ, capture_output=True, text=True, encoding='utf-8').stdout
    nuevos = subprocess.run(['git', 'ls-files', '--others', '--exclude-standard'], cwd=RAIZ, capture_output=True, text=True).stdout.split()
    return a + b, nuevos


def analizar(diff, nuevos=()):
    motivos, archivos, lineas = [], set(nuevos), 0
    actual = None
    for l in diff.splitlines():
        if l.startswith('+++ '):
            actual = l[6:] if l.startswith('+++ b/') else None
            if actual: archivos.add(actual)
            continue
        if l.startswith('--- '):
            if l.startswith('--- a/'): archivos.add(l[6:])
            continue
        if not actual and not l.startswith(('+', '-')): continue
        if l.startswith(('+', '-')) and not l.startswith(('+++', '---')):
            lineas += 1
            cuerpo = l[1:]
            f = actual or ''
            if f.endswith('src/lib/config.ts') or f.startswith('src/lib/config'):
                if re.search(r"\bcoverage\s*:", cuerpo):
                    motivos.append(f'{f}: cambia un mapa coverage (alcance territorial): {cuerpo.strip()[:100]}')
                if re.search(r"\?v=\d+", cuerpo):
                    motivos.append(f'{f}: cambia un cache-buster ?v (cambio de datos): {cuerpo.strip()[:100]}')
                if re.search(r"https?://", cuerpo):
                    motivos.append(f'{f}: cambia una URL: {cuerpo.strip()[:100]}')
                if re.search(r"\bid\s*:\s*'[a-z0-9_]+'", cuerpo):
                    motivos.append(f'{f}: agrega o quita un id de capa o análisis: {cuerpo.strip()[:100]}')
                if l.startswith('-') and re.search(r"^\t[a-z0-9_]+\s*:\s*\{", cuerpo):
                    motivos.append(f'{f}: elimina una entrada de registro: {cuerpo.strip()[:100]}')
            if f.startswith('src/') and l.startswith('+'):
                for dom in re.findall(r"https?://([a-z0-9.-]+)", cuerpo):
                    if not re.search(r'(spatia\.ar|r2\.dev|w3\.org|openstreetmap|maplibre|creativecommons|conicet|unam|github\.com)', dom):
                        motivos.append(f'{f}: dominio externo nuevo {dom}')
    for f in sorted(archivos):
        if f in ('/dev/null',): continue
        if not any(f == p or f.startswith(p) for p in R['rutas_permitidas']):
            motivos.append(f'archivo fuera de lo permitido: {f}')
        for p in R.get('rutas_prohibidas_en_src', []):
            if f.startswith(p): motivos.append(f'archivo de datos generado por el pipeline: {f}')
    if lineas > R['max_lineas_cambiadas']:
        motivos.append(f'{lineas} líneas cambiadas (tope {R["max_lineas_cambiadas"]})')
    return motivos, sorted(archivos), lineas


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == '--diff':
        diff, nuevos = Path(sys.argv[2]).read_text(encoding='utf-8'), []
    else:
        diff, nuevos = diff_git(sys.argv[1] if len(sys.argv) > 1 else 'origin/main')
    motivos, archivos, lineas = analizar(diff, nuevos)
    print(f'{len(archivos)} archivos, {lineas} líneas')
    if motivos:
        print('RECHAZADO:'); [print('  -', m) for m in dict.fromkeys(motivos)]
        sys.exit(1)
    print('PERMITIDO'); sys.exit(0)


if __name__ == '__main__':
    main()
