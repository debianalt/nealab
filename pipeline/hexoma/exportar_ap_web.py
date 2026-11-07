"""Hexoma → spatia.ar: bordes de las áreas protegidas y sitios Ramsar del NEA argentino, para el mapa y su hover.

La web dibuja sólo el borde y, al pasar el mouse, dice qué es cada área y cuánto ocupa. No interpreta qué se puede
hacer adentro: cada área tiene su norma y eso no es tema de la plataforma (decisión del autor, 7-nov-2026).

Fuente: `institucional.areas_protegidas` (build_hexoma_institucional.py). Es lo último que publica Ambiente de Nación:
SIFAP 2016 (nacionales: ordenamiento:apn_sifal2016; provinciales: ordenamiento:app_polig_2016b) y Ramsar
(areas_protegidas:sitios_ramsar). Las áreas creadas después de 2016 no figuran; el hover lo dice.
Sólo entran los polígonos que tocan celdas del NEA argentino (institucional.ap_h3).

Propiedades: nombre, tipo (nacional | provincial | ramsar), categoria (como la publica la fuente), creacion,
instrumento, ha (superficie declarada si la fuente la trae; si no, la del polígono), fuente.

Salida: pipeline/output/hexoma/web/areas_protegidas_v{N}.geojson → R2 data/overlays/areas_protegidas_v{N}.geojson
(AP_OVERLAY_URL en src/lib/config.ts). Para actualizar con una fuente más nueva (p. ej. IMiBio para Misiones):
sumarla en build_hexoma_institucional.py, correr este script con --version N+1, subir y cambiar la URL.

Uso (desde neahub/):
    python pipeline/hexoma/exportar_ap_web.py [--version 1]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'pipeline' / 'output' / 'hexoma' / 'web'
FUENTE = {'nacional': 'SIFAP 2016', 'provincial': 'SIFAP 2016', 'ramsar': 'Ramsar (Ambiente de Nación)'}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--version', type=int, default=1)
    a = ap.parse_args()
    conn = connect(DB)
    cur = conn.cursor()
    # ≈ 30 m de tolerancia (0,0003°); 6 decimales en las coordenadas (≈ 0,1 m) alcanzan.
    cur.execute("""
        SELECT p.ap_id, p.tipo, p.nombre, p.categoria, p.creacion, p.instrumento, p.sup_declarada_ha,
               ST_Area(p.geom::geography) / 1e4 AS ha_poligono,
               ST_AsGeoJSON(ST_SimplifyPreserveTopology(p.geom, 0.0003), 6) AS g
        FROM institucional.areas_protegidas p
        WHERE p.tipo IN ('nacional', 'provincial', 'ramsar')
          AND p.ap_id IN (SELECT DISTINCT ap_id FROM institucional.ap_h3)
        ORDER BY ST_Area(p.geom::geography) DESC""")
    feats = []
    for ap_id, tipo, nombre, cat, creacion, instr, sup, ha_pol, g in cur.fetchall():
        try:
            sup_f = float(sup) if sup not in (None, '') else None
        except ValueError:
            sup_f = None
        ha = sup_f if sup_f and sup_f > 0 else ha_pol
        feats.append({'type': 'Feature', 'geometry': json.loads(g), 'properties': {
            'id': int(ap_id), 'nombre': (nombre or '').strip(), 'tipo': tipo, 'categoria': (cat or '').strip(),
            'creacion': (str(creacion).strip().removesuffix('.0') if creacion else ''), 'instrumento': (instr or '').strip(),
            'ha': round(ha), 'ha_declarada': sup_f is not None and sup_f > 0, 'fuente': FUENTE[tipo]}})
    OUT.mkdir(parents=True, exist_ok=True)
    f = OUT / f'areas_protegidas_v{a.version}.geojson'
    f.write_text(json.dumps({'type': 'FeatureCollection', 'features': feats}, ensure_ascii=False,
                            separators=(',', ':')), encoding='utf-8')
    por = {}
    for x in feats:
        por[x['properties']['tipo']] = por.get(x['properties']['tipo'], 0) + 1
    log(f'{len(feats)} áreas {por}; {f.stat().st_size / 1e6:.2f} MB → {f}')


if __name__ == '__main__':
    main()
