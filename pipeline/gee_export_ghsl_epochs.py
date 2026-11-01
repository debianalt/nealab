"""Exporta la superficie construida de GHSL por época para el NEA argentino (cruce censal del Hexoma).

GHS-BUILT-S R2023A (JRC/GHSL/P2023A/GHS_BUILT_S/<época>, banda built_surface: m² construidos
por píxel de 100 m) en las épocas de los censos: 1990 (censo 1991), 2000 (2001), 2010 (2010) y
2020 (2022). Sirve de peso de época para repartir cada radio entre sus celdas: los edificios de
Open Buildings son de ~2023 y no pueden repartir la población de 1991 sin anacronismo.

Una imagen por época, del rectángulo que cubre los radios de los cuatro censos de Misiones,
Corrientes, Chaco y Formosa, a 100 m en EPSG:4326, a gs://spatia-satellite/satellite/nea_ar/.
Después: python pipeline/hexoma/ghsl_a_h3.py

Uso:
    python pipeline/gee_export_ghsl_epochs.py            # espera a que terminen
    python pipeline/gee_export_ghsl_epochs.py --no-wait
"""
import argparse
import sys
import time

import ee

from gee_export_activity_rasters import authenticate, ghsl_built

EPOCAS = [1990, 2000, 2010, 2020]
# radios de los censos 1991-2022 del NEA (ST_Extent en hexoma, 1-nov-2026) + 0,05° de margen
BBOX = [-63.48, -30.79, -53.58, -22.41]
SCALE = 100
BUCKET = 'spatia-satellite'
PREFIX = 'satellite/nea_ar'


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--no-wait', action='store_true')
    args = ap.parse_args()
    authenticate()
    region = ee.Geometry.Rectangle(BBOX)
    tasks = []
    for e in EPOCAS:
        img = ghsl_built(e).clip(region).round().toUint16()  # m² por píxel, 0–10000
        name = f'ghsl_built_{e}'
        t = ee.batch.Export.image.toCloudStorage(
            image=img, description=f'nea_ar_{name}', bucket=BUCKET, fileNamePrefix=f'{PREFIX}/{name}',
            region=region, scale=SCALE, crs='EPSG:4326', maxPixels=1e10,
            formatOptions={'cloudOptimized': True})
        t.start()
        tasks.append((name, t))
        print(f'  iniciada: {name}', flush=True)
    if args.no_wait:
        return 0
    while True:
        st = [(n, t.status()['state']) for n, t in tasks]
        corriendo = [n for n, s in st if s in ('READY', 'RUNNING')]
        if not corriendo:
            break
        print(f'  [{len(corriendo)} corriendo]', flush=True)
        time.sleep(30)
    ok = True
    for n, t in tasks:
        s = t.status()
        ok &= s['state'] == 'COMPLETED'
        print(f"  {n}: {s['state']} {s.get('error_message', '')}")
    print(f'\nSiguiente: gcloud storage cp "gs://{BUCKET}/{PREFIX}/ghsl_built_*.tif" pipeline/output/hexoma/ghsl/')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
