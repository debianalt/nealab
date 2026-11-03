"""Une los hallazgos de las sondas, descarta los ignorados y resume.

Uso: python director/unir.py DIR_SONDAS SALIDA.json [--comparar BASE.json]
  DIR_SONDAS: carpeta con hallazgos_*.json (recorre subcarpetas: artefactos de cada job).
  --comparar: hallazgos de referencia; imprime arreglados / nuevos / siguen (para la compuerta).
"""
import argparse, json, sys
from pathlib import Path
import yaml

DIRECTOR = Path(__file__).parent
ORDEN = {'alta': 0, 'media': 1, 'baja': 2}


def cargar(dir_):
    items = {}
    for f in sorted(Path(dir_).rglob('hallazgos_*.json')):
        for h in json.loads(f.read_text(encoding='utf-8')):
            if 'captura' in h:
                h['captura'] = str((f.parent / h['captura']).relative_to(Path(dir_))).replace('\\', '/')
            items[h['huella']] = h
    return items


def ignorados():
    d = yaml.safe_load((DIRECTOR / 'ignorar.yaml').read_text(encoding='utf-8')) or []
    return {x['huella'] for x in d if isinstance(x, dict) and x.get('huella')}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('dir'); ap.add_argument('salida'); ap.add_argument('--comparar')
    a = ap.parse_args()
    items = cargar(a.dir)
    ign = ignorados()
    lista = sorted((h for k, h in items.items() if k not in ign), key=lambda h: (ORDEN.get(h['severidad'], 9), h['tipo'], h.get('capa') or '', h.get('territorio') or ''))
    Path(a.salida).write_text(json.dumps(lista, ensure_ascii=False, indent=1), encoding='utf-8')
    cuenta = {}
    for h in lista: cuenta[h['severidad']] = cuenta.get(h['severidad'], 0) + 1
    print(f"{len(lista)} hallazgos ({', '.join(f'{k}: {v}' for k, v in sorted(cuenta.items(), key=lambda x: ORDEN.get(x[0], 9)))}); {len(items) - len(lista)} ignorados")
    if a.comparar:
        base = {h['huella']: h for h in json.loads(Path(a.comparar).read_text(encoding='utf-8'))}
        nuevos = [h for k, h in items.items() if k not in base and k not in ign]
        resultado = {'nuevos': nuevos, 'siguen': [k for k in items if k in base], 'arreglados': [k for k in base if k not in items]}
        Path(a.salida).with_suffix('.comparacion.json').write_text(json.dumps(resultado, ensure_ascii=False, indent=1), encoding='utf-8')
        print(f"comparación: {len(resultado['arreglados'])} arreglados, {len(resultado['siguen'])} siguen, {len(nuevos)} nuevos")
        for h in nuevos: print('  NUEVO', h['severidad'], h['tipo'], h.get('territorio'), h.get('capa'), h['detalle'][:160])


if __name__ == '__main__':
    main()
