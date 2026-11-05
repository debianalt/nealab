#!/usr/bin/env python3
"""
Validate the minutes-by-road accessibility layer (compute_accessibility_h3.py, 5-nov-2026).

Checks, per published territory:
  1. hospital >= salud in every cell (hospitals are a subset of the health set).
  2. capital: new vs previous run (`sat_accessibility.pre-20261105.parquet`); the only
     intended change is the east–west scale fix of the single-source code (~12 % at 27 S).
  3. salud vs c_healthcare_time of the health layer (Oxford/OSM, per census tract, AR),
     read from the Hexoma (v_hexoma), on populated cells.
  4. Known places: the six values at the cell containing each point.
  5. Old straight-line km vs new minutes (rank correlation), as a sanity check.

Writes pipeline/output/access_sources/validacion.md.

    python pipeline/validar_accesibilidad.py --output-dir <pipeline output root>
"""
import argparse
import os
import sys

import h3
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import OUTPUT_DIR, get_territory  # noqa: E402

TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa', 'itapua_py', 'alto_parana_py']
COLS = ['travel_min_capital_raw', 'travel_min_cabecera_raw', 'travel_min_hospital_raw',
        'travel_min_salud_raw', 'travel_min_secundaria_raw', 'travel_min_ruta_raw']
PLACES = [
    ('misiones', 'Posadas, centro', -27.3671, -55.8961),
    ('misiones', 'San Pedro', -26.6217, -54.1083),
    ('misiones', 'Bernardo de Irigoyen', -26.2553, -53.6464),
    ('corrientes', 'Esquina', -30.0144, -59.5272),
    ('corrientes', 'Esteros del Iberá (Colonia Carlos Pellegrini)', -28.5376, -57.1721),
    ('chaco', 'Resistencia, centro', -27.4513, -58.9867),
    ('chaco', 'El Impenetrable (Misión Nueva Pompeya)', -24.9333, -61.4833),
    ('formosa', 'Ingeniero Juárez', -23.8986, -61.8500),
    ('itapua_py', 'Encarnación, centro', -27.3360, -55.8690),
]


def spearman(a, b):
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 10:
        return float('nan'), int(m.sum())
    return float(pd.Series(a[m]).rank().corr(pd.Series(b[m]).rank())), int(m.sum())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output-dir', default=OUTPUT_DIR)
    args = ap.parse_args()
    root = args.output_dir
    lines = ['# Validación de accesibilidad en minutos por camino', '',
             f'Generado por `pipeline/validar_accesibilidad.py` sobre `{root}`.', '']

    hexoma = None
    try:
        import psycopg2
        con = psycopg2.connect('postgresql://postgres@localhost:5432/hexoma')
        hexoma = pd.read_sql("SELECT h3, territorio, health_access__c_healthcare_time AS hc, "
                             "accessibility__dist_nearest_hospital_km AS km_hosp, "
                             "accessibility__dist_nearest_secundaria_km AS km_sec, "
                             "accessibility__dist_primary_m AS km_ruta FROM v_hexoma", con)
    except Exception as e:  # the Hexoma is local and optional
        lines.append(f'(Hexoma no disponible: {e})\n')

    lines += ['## 1–3. Por territorio', '',
              '| Territorio | celdas | hospital < salud | capital nueva/vieja (mediana, P10–P90) | '
              'salud vs tiempo a salud de la capa de salud (Spearman, n) | mediana salud (min) | '
              'mediana hospital | mediana secundaria | mediana ruta |',
              '|---|---|---|---|---|---|---|---|---|']
    frames = {}
    for t in TERRITORIES:
        prefix = get_territory(t)['output_prefix'].rstrip('/')
        d = os.path.join(root, prefix) if prefix else root
        new = pd.read_parquet(os.path.join(d, 'sat_accessibility.parquet'))
        old = pd.read_parquet(os.path.join(d, 'sat_accessibility.pre-20261105.parquet'))
        frames[t] = new
        bad = int((new.travel_min_hospital_raw < new.travel_min_salud_raw).sum())
        j = new.merge(old[['h3index', 'travel_min_capital_raw']], on='h3index', suffixes=('', '_old'))
        ok = (j.travel_min_capital_raw_old > 30)
        r = (j.travel_min_capital_raw[ok] / j.travel_min_capital_raw_old[ok])
        cap = f'{r.median():.3f} ({r.quantile(.1):.3f}–{r.quantile(.9):.3f})' if len(r) else '—'
        sp = '—'
        if hexoma is not None and not t.endswith('_py'):
            hx = hexoma[hexoma.territorio == t].merge(new, left_on='h3', right_on='h3index')
            rho, n = spearman(hx.travel_min_salud_raw.to_numpy(float), hx.hc.to_numpy(float))
            sp = f'{rho:.2f} (n {n:,})'
        med = lambda c: '—' if new[c].isna().all() else f'{new[c].median():.0f}'
        lines.append(f'| {t} | {len(new):,} | {bad} | {cap} | {sp} | {med("travel_min_salud_raw")} | '
                     f'{med("travel_min_hospital_raw")} | {med("travel_min_secundaria_raw")} | '
                     f'{med("travel_min_ruta_raw")} |')

    lines += ['', '## 4. Lugares conocidos (minutos)', '',
              '| Lugar | capital | ciudad ≥50k | hospital | salud | secundaria | ruta |',
              '|---|---|---|---|---|---|---|']
    for t, name, lat, lng in PLACES:
        cell = h3.latlng_to_cell(lat, lng, 9)
        row = frames[t][frames[t].h3index == cell]
        if row.empty:
            lines.append(f'| {name} | (celda {cell} fuera de la grilla) |||||')
            continue
        v = row.iloc[0]
        f = lambda c: '—' if pd.isna(v[c]) else f'{v[c]:.0f}'
        lines.append(f'| {name} | ' + ' | '.join(f(c) for c in COLS) + ' |')

    if hexoma is not None:
        lines += ['', '## 5. Km en línea recta (capa anterior) contra minutos nuevos (Spearman)', '',
                  '| Territorio | hospital | secundaria | ruta |', '|---|---|---|---|']
        for t in TERRITORIES:
            if t.endswith('_py'):
                continue
            hx = hexoma[hexoma.territorio == t].merge(frames[t], left_on='h3', right_on='h3index')
            cells = [spearman(hx[a].to_numpy(float), hx[b].to_numpy(float))[0]
                     for a, b in [('km_hosp', 'travel_min_hospital_raw'),
                                  ('km_sec', 'travel_min_secundaria_raw'),
                                  ('km_ruta', 'travel_min_ruta_raw')]]
            lines.append(f'| {t} | ' + ' | '.join(f'{c:.2f}' for c in cells) + ' |')

    out = os.path.join(root, 'access_sources', 'validacion.md')
    with open(out, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(lines) + '\n')
    print('\n'.join(lines))
    return 0


if __name__ == '__main__':
    sys.exit(main())
