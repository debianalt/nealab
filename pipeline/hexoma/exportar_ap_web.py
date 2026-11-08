"""Áreas protegidas del NEA argentino para el mapa de spatia.ar: bordes y hover (v2, fuentes provinciales e IGN).

La web dibuja sólo el borde y, al pasar el mouse, dice qué es cada área, cuánto ocupa, su norma y de dónde sale. No
interpreta qué se puede hacer adentro: cada área tiene su norma (decisión del autor, 7-nov-2026).

Fuentes (verificadas el 7-nov-2026; SIFAP 2016 de Ambiente de Nación quedó viejo):
- Misiones: Fundación Vida Silvestre Argentina, capa «misiones-areas-protegidas» (ArcGIS Online, mar-2026, 116 áreas),
  que reproduce el mapa del Sistema Provincial de ANP de la Dirección de Áreas Naturales Protegidas (Ministerio de
  Ecología y RNR de Misiones, mayo 2025). Se cita a las dos (indicación del autor). Alternativa: la capa oficial
  geonode:anp_nov2023_wgs84 del GeoServer de Ordenamiento Territorial de Misiones (IDERA), con --misiones oficial.
- Corrientes: IDE Corrientes, workspace areas_protegidas_ide (nacionales, provinciales, municipales, privadas, Ramsar),
  licencia CC BY-NC-SA 4.0 (pie de ide.corrientes.gob.ar).
- Chaco, Formosa, y en Misiones y Corrientes lo que la fuente provincial no tiene (p. ej. PN Mburucuyá, reservas del
  INTA o de la Defensa): IGN, ign:area_protegida (WFS wms.ign.gob.ar), reutilización libre por la Ley 27.275.
  Un área del IGN entra en Misiones o Corrientes sólo si menos del 50 % de su superficie cae dentro de las áreas de la
  fuente provincial.

Propiedades: nombre, tipo (nacional | provincial | municipal | privada | ramsar | biosfera, sólo para el estilo),
categoria (texto de la fuente), ha (la declarada si la fuente la trae y coincide ± 50 % con la del polígono; si no,
la del polígono, con ha_declarada=false), norma (texto de la fuente), fuente, licencia.

Las bajadas quedan en %HEXOMA_FUENTES%/areas_protegidas/<fecha>/ con sha256 en fuentes.json.
Salida: pipeline/output/hexoma/web/areas_protegidas_v{N}.geojson → R2 data/overlays/areas_protegidas_v{N}.geojson
(AP_OVERLAY_URL en src/lib/config.ts). Para actualizar: correr de nuevo con --version N+1, subir y cambiar la URL.

Uso (desde neahub/):
    python pipeline/hexoma/exportar_ap_web.py [--version 2] [--misiones vidasilvestre|oficial]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests
import truststore

# La IDE Corrientes no manda el certificado intermedio: el almacén del sistema lo completa (sin desactivar la
# verificación).
truststore.inject_into_ssl()
from pyproj import Geod
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'pipeline' / 'output' / 'hexoma' / 'web'
FUENTES = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'areas_protegidas'
UA = {'User-Agent': 'spatia-pipeline/1.0'}
GEOD = Geod(ellps='WGS84')
PROVINCIAS = {'Misiones', 'Corrientes', 'Chaco', 'Formosa'}

FVSA = ('https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/' + quote('Áreas_protegidas')
        + '/FeatureServer/23/query?where=1%3D1&outFields=*&outSR=4326&f=geojson')
MIS_OFICIAL = ('https://ide.ordenamientoterritorial.misiones.gob.ar/geoserver/ows?service=WFS&version=2.0.0'
               '&request=GetFeature&typeNames=geonode:anp_nov2023_wgs84&outputFormat=application/json&srsName=EPSG:4326')
CTES = 'https://geoportal.corrientes.gob.ar/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json&srsName=EPSG:4326&typeNames=areas_protegidas_ide:'
CTES_CAPAS = ['areas_protegidas_nacionales', 'areas_protegidas_provinciales', 'Reserva_Natural_Municipal',
              'reserva_privada', 'Sitio_Ramsar']
IGN = 'https://wms.ign.gob.ar/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json&srsName=EPSG:4326&typeNames='

FUENTE_TXT = {
    'vidasilvestre': 'Fundación Vida Silvestre Argentina (mar-2026), sobre el mapa de ANP de la Dirección de Áreas '
                     'Naturales Protegidas, Ministerio de Ecología de Misiones (2025)',
    'oficial': 'Provincia de Misiones, IDE Ordenamiento Territorial (nov-2023)',
    'corrientes': 'IDE Corrientes (2025)',
    'ign': 'IGN, Capas SIG',
}
LICENCIA = {'vidasilvestre': '', 'oficial': '', 'corrientes': 'CC BY-NC-SA 4.0', 'ign': 'Ley 27.275'}


def bajar(url: str, nombre: str, carpeta: Path, registro: list) -> dict:
    for intento in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=180)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            if intento == 2:
                raise
            print(f'  reintento {nombre}: {e}')
            time.sleep(5)
    f = carpeta / f'{nombre}.geojson'
    f.write_bytes(r.content)
    registro.append({'nombre': nombre, 'url': url, 'bytes': len(r.content),
                     'sha256': hashlib.sha256(r.content).hexdigest()})
    return r.json()


def ha_geod(g) -> float:
    return abs(GEOD.geometry_area_perimeter(g)[0]) / 1e4


def arreglar(s):
    """UTF-8 leído como latin-1 («ZanjÃ³n»): se revierte sólo si el resultado es UTF-8 válido."""
    if not isinstance(s, str) or 'Ã' not in s:
        return s
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def tipo_de(cat: str, dominio: str = '') -> str:
    t = f'{cat} {dominio}'.lower()
    if 'ramsar' in t:
        return 'ramsar'
    if 'biósfera' in t or 'biosfera' in t:
        return 'biosfera'
    if 'privad' in t:
        return 'privada'
    if 'municipal' in t:
        return 'municipal'
    if 'nacional' in t or 'federal' in t or 'defensa' in t:
        return 'nacional'
    return 'provincial'


def titulo(s: str) -> str:
    """«PARQUE PROVINCIAL» → «Parque provincial»."""
    s = (s or '').strip()
    return s[:1].upper() + s[1:].lower() if s.isupper() else s


def area(nombre, cat, dominio, norma, g, ha_decl, fuente, prov):
    ha_pol = ha_geod(g)
    ok = ha_decl is not None and ha_decl > 0 and 0.5 <= ha_decl / max(ha_pol, 1e-9) <= 1.5
    return {'geom': g, 'prov': prov, 'props': {
        'nombre': re.sub(r'\s+', ' ', (nombre or '').replace('"', '')).strip(), 'tipo': tipo_de(cat, dominio),
        'categoria': titulo(cat), 'ha': round(ha_decl if ok else ha_pol), 'ha_declarada': bool(ok),
        'norma': re.sub(r'\s+', ' ', (norma or '').strip()), 'fuente': FUENTE_TXT[fuente], 'licencia': LICENCIA[fuente]}}


CATEGORIAS = r'(parque|reserva|refugio|paisaje|monumento|sitio|ramsar|natural|nacional|provincial|municipal|privad[oa]|'              r'de|del|la|las|los|vida|silvestre|uso|usos|multiples?|ecologica|cultural|prov|nac|el|y)'


def nucleo(nombre: str) -> str:
    """Nombre sin las palabras de categoría, sin tildes ni signos: «Parque Municipal Zanjón de Loreto» → «zanjonloreto»."""
    import unicodedata
    t = unicodedata.normalize('NFKD', (nombre or '').lower()).encode('ascii', 'ignore').decode()
    t = re.sub(r'[^a-z0-9 ]', ' ', t)
    return ''.join(w for w in t.split() if not re.fullmatch(CATEGORIAS, w))


def norma_ign(fdc: str) -> str:
    """El fdc del IGN encadena la fuente y la norma («SIB/IGN/Ley Nacional Nº 26.996/2014»): queda la norma."""
    partes = [x.strip() for x in (fdc or '').split('/')]
    while partes and re.fullmatch(r'(SIB|IGN|IDE ?CHACO|IDE ?Chaco|Mapa Pcial\. IGN)', partes[0], re.I):
        partes.pop(0)
    return '/'.join(partes)


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def recolectar(misiones: str, carpeta: Path) -> tuple[list, list, dict]:
    """Baja las fuentes a `carpeta`, une y deduplica. Devuelve (áreas con geometría completa ordenadas por superficie,
    registro de archivos bajados, áreas del IGN por provincia). La usan este script (GeoJSON web) y
    build_hexoma_institucional.py (institucional.areas_protegidas): una sola fuente de verdad."""
    carpeta.mkdir(parents=True, exist_ok=True)
    reg: list = []
    areas: list = []

    # Misiones
    if misiones == 'vidasilvestre':
        d = bajar(FVSA, 'misiones_vidasilvestre', carpeta, reg)
        for f in d['features']:
            p = f['properties']
            areas.append(area(p.get('nombre_c_1'), p.get('clasificac'), p.get('dominio'), p.get('creado_x1'),
                              shape(f['geometry']), num(p.get('sup_ley_ha')), 'vidasilvestre', 'Misiones'))
    else:
        d = bajar(MIS_OFICIAL, 'misiones_oficial_2023', carpeta, reg)
        for f in d['features']:
            p = f['properties']
            areas.append(area(p.get('nombre_c_1'), p.get('clasificac'), p.get('dominio'), p.get('creado_x1'),
                              shape(f['geometry']), None, 'oficial', 'Misiones'))
    # Corrientes
    for capa in CTES_CAPAS:
        d = bajar(CTES + capa, f'corrientes_{capa}', carpeta, reg)
        for f in d['features']:
            p = {k.lower(): arreglar(v) for k, v in f['properties'].items()}
            areas.append(area(p.get('fna') or p.get('nam'), p.get('gna') or p.get('tap'), '', '',
                              shape(f['geometry']), None, 'corrientes', 'Corrientes'))
    # IGN: provincias y áreas protegidas en el bbox del NEA
    # Sin bbox: con EPSG:4326 el GeoServer del IGN invierte los ejes. Provincias por nombre; áreas, la capa entera.
    cql = '&CQL_FILTER=' + quote("nam IN ('Misiones','Corrientes','Chaco','Formosa')")
    prov = bajar(IGN + 'ign:provincia' + cql, 'ign_provincia', carpeta, reg)
    provs = {f['properties']['nam']: shape(f['geometry']) for f in prov['features']
             if f['properties'].get('nam') in PROVINCIAS}
    assert set(provs) == PROVINCIAS, f'provincias del IGN: {sorted(provs)}'
    d = bajar(IGN + 'ign:area_protegida', 'ign_area_protegida', carpeta, reg)
    provinciales = {p: unary_union([x['geom'].buffer(0) for x in areas if x['prov'] == p])
                    for p in ('Misiones', 'Corrientes')}
    nombres = {p: [(nucleo(x['props']['nombre']), x['geom'].buffer(0)) for x in areas if x['prov'] == p]
               for p in provinciales}
    n_ign = {}
    for f in d['features']:
        if not f.get('geometry'):
            continue
        g = shape(f['geometry'])
        pt = g.representative_point()
        donde = next((n for n, pg in provs.items() if pg.contains(pt)), None)
        if donde is None:
            continue
        p = f['properties']
        if donde in provinciales:
            gb = g.buffer(0)
            if gb.area and gb.intersection(provinciales[donde]).area / gb.area >= 0.5:
                continue
            # La misma área con otro polígono (IDE Corrientes 106 ha, IGN 1.212 ha): manda la fuente provincial.
            # Mismo nombre y superposición ≥ 1 % del más chico (la Defensa y el parque provincial Puerto Península
            # son áreas contiguas distintas).
            k = nucleo(p.get('fna') or p.get('nam'))
            if k and any((k in n or n in k) and gb.intersection(pg).area >= 0.01 * min(gb.area, pg.area)
                         for n, pg in nombres[donde] if len(n) >= 5):
                continue
        areas.append(area(p.get('fna') or p.get('nam'), p.get('gna') or '', '', norma_ign(p.get('fdc')), g, None, 'ign', donde))
        n_ign[donde] = n_ign.get(donde, 0) + 1

    return sorted(areas, key=lambda x: -x['props']['ha']), reg, n_ign


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--version', type=int, default=2)
    ap.add_argument('--misiones', choices=['vidasilvestre', 'oficial'], default='vidasilvestre')
    a = ap.parse_args()
    hoy = dt.date.today().isoformat()
    carpeta = FUENTES / hoy
    areas, reg, n_ign = recolectar(a.misiones, carpeta)

    feats = []
    for x in areas:
        g = x['geom'].simplify(0.0003, preserve_topology=True)
        feats.append({'type': 'Feature', 'geometry': mapping(g), 'properties': {'id': len(feats) + 1, **x['props']}})
    OUT.mkdir(parents=True, exist_ok=True)
    out = OUT / f'areas_protegidas_v{a.version}.geojson'
    out.write_text(json.dumps({'type': 'FeatureCollection', 'features': feats}, ensure_ascii=False,
                              separators=(',', ':')), encoding='utf-8')
    (carpeta / 'fuentes.json').write_text(json.dumps({'fecha': hoy, 'misiones': a.misiones, 'archivos': reg},
                                                     ensure_ascii=False, indent=1), encoding='utf-8')
    por = {}
    for x in areas:
        k = (x['prov'], x['props']['fuente'][:22])
        por[k] = por.get(k, 0) + 1
    for k in sorted(por):
        print(f'  {k[0]:<11} {k[1]:<24} {por[k]}')
    print(f'{len(feats)} áreas; del IGN por provincia {n_ign}; {out.stat().st_size / 1e6:.2f} MB → {out}')


if __name__ == '__main__':
    main()
