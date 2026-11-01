"""Baja las series diarias de las estaciones del INTA (SIGA) del NEA para validar el clima del Hexoma.

Usa la misma API pública que la aplicación https://siga.inta.gob.ar/ (js/urlserver.js):
    lista de estaciones   CdnaUV0iiERRpFQE.php?param_type=estacion&param_value=
    datos diarios         CdnaUV0iiERRpFQE.php?param_type=diario&param_value=<id>/<D-M-AAAA>/<D-M-AAAA>
(las rutas http:// y document/series/*.xls del paquete R `siga` ya no responden).

Guarda en %HEXOMA_FUENTES%/estaciones/:
    siga_original/estaciones.json              la lista tal cual
    siga_original/<id>/<año>.json              cada año tal cual lo da la API
    extra/siga_<id>_<nombre>.csv               fecha, tmin, tmax, p, tmin_suelo (lo que lee validar_clima.py)
    extra/siga_<id>_<nombre>.json              lat, lng, elev, tipo, período
Columnas de origen: tempAbrigo150Min/Max (abrigo a 1,5 m, °C), precDiaPlub (mm), tempInte5Min
(mínima a la intemperie a 5 cm, °C: helada a nivel del suelo; sólo convencionales).

Pide de a un año por estación, de a 2 pedidos a la vez. Idempotente: no vuelve a pedir años cerrados
ya bajados (el año en curso se vuelve a pedir).

Uso (desde neahub/):
    python pipeline/hexoma/bajar_siga.py                       # Misiones, Corrientes, Chaco, Formosa
    python pipeline/hexoma/bajar_siga.py --provincias Misiones
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
EST = FUENTES_DIR / 'estaciones'
ORIG = EST / 'siga_original'
EXTRA = EST / 'extra'
API = 'https://siga.inta.gob.ar/CdnaUV0iiERRpFQE.php?param_type={}&param_value={}'
NEA = ['Misiones', 'Corrientes', 'Chaco', 'Formosa']


def get(url: str) -> bytes:
    for intento in range(4):
        r = subprocess.run(['curl', '-s', '-f', '-m', '120', url], capture_output=True)
        if r.returncode == 0:
            return r.stdout
        time.sleep(5 * (intento + 1))
    raise RuntimeError(f'falló {url}')


def slug(s: str) -> str:
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


def fecha(s: str | None) -> pd.Timestamp | None:
    return pd.Timestamp(s.replace('Z[UTC]', '')) if s else None


def bajar_anio(eid: int, anio: int, cerrado: bool) -> Path:
    f = ORIG / str(eid) / f'{anio}.json'
    if f.exists() and cerrado:
        return f
    b = get(API.format('diario', f'{eid}/1-1-{anio}/31-12-{anio}'))
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(b or b'[]')
    time.sleep(0.5)
    return f


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--provincias', nargs='+', default=NEA)
    args = ap.parse_args()
    ORIG.mkdir(parents=True, exist_ok=True)
    EXTRA.mkdir(parents=True, exist_ok=True)
    lista = json.loads(get(API.format('estacion', '')))
    (ORIG / 'estaciones.json').write_text(json.dumps(lista, ensure_ascii=False, indent=1), encoding='utf-8')
    sel = [e for e in lista if e.get('provincia') in args.provincias and e.get('minimoDiario')]
    print(f'{len(lista)} estaciones en SIGA; {len(sel)} con datos diarios en {", ".join(args.provincias)}', flush=True)
    hoy = pd.Timestamp.today()
    resumen = []
    for e in sel:
        d0, d1 = fecha(e['minimoDiario']), fecha(e['maximoDiario'])
        anios = range(d0.year, d1.year + 1)
        with ThreadPoolExecutor(2) as ex:
            archivos = list(ex.map(lambda a: bajar_anio(e['id'], a, a < hoy.year), anios))
        filas = []
        for f in archivos:
            filas += json.loads(f.read_text(encoding='utf-8') or '[]')
        if not filas:
            print(f"  {e['nombre']}: sin filas", flush=True)
            continue
        df = pd.DataFrame(filas)
        out = pd.DataFrame({
            'fecha': pd.to_datetime(df.fechaHora.str.replace('Z[UTC]', '', regex=False)).dt.strftime('%Y-%m-%d'),
            'tmin': df.get('tempAbrigo150Min'), 'tmax': df.get('tempAbrigo150Max'),
            'p': df.get('precDiaPlub'), 'tmin_suelo': df.get('tempInte5Min')})
        out = out.drop_duplicates('fecha').sort_values('fecha')
        nombre = f"siga_{e['id']}_{slug(e['nombre'])}"
        out.to_csv(EXTRA / f'{nombre}.csv', index=False)
        meta = {'lat': e['latitud'], 'lng': e['longitud'], 'elev': e.get('altura'), 'id_siga': e['id'],
                'id_interno': e.get('idInterno'), 'nombre': e['nombre'], 'provincia': e['provincia'],
                'tipo': e.get('tipo'), 'id_tipo': e.get('idTipo'), 'desde': str(d0.date()), 'hasta': str(d1.date()),
                'fuente': 'INTA SIGA', 'bajado': str(hoy.date())}
        (EXTRA / f'{nombre}.json').write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding='utf-8')
        cob = out.tmin.notna().mean() * 100
        resumen.append((e['provincia'], e['nombre'], e.get('tipo'), e.get('altura'), out.fecha.min(), out.fecha.max(),
                        len(out), round(cob, 0), int(out.tmin_suelo.notna().sum())))
        print(f"  {e['provincia']} · {e['nombre']}: {len(out):,} días, tmin {cob:.0f} %", flush=True)
    r = pd.DataFrame(resumen, columns=['provincia', 'estacion', 'tipo', 'altura_m', 'desde', 'hasta', 'dias',
                                       'pct_tmin', 'dias_tmin_suelo'])
    (EXTRA / 'LEEME.md').write_text('# Estaciones INTA SIGA bajadas por pipeline/hexoma/bajar_siga.py\n\n'
                                     + r.to_markdown(index=False) + '\n', encoding='utf-8')
    print(r.to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
