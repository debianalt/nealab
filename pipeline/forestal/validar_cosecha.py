"""Validación de la marca «pérdida después de plantar» (cosecha_hansen.py) → pipeline/forestal/validacion_cosecha.md.

Pruebas: (1) por clase de edad del inventario, la superficie marcada debería ser baja en macizos jóvenes y crecer con
la edad (a más edad, más chance de que el macizo ya se haya talado); (2) los macizos que el DNDFI declara «Afectada
por incendios» deberían marcar mucho más que los «Productivo»; (3) el año de la pérdida en esos incendiados debería
concentrarse en 2022 (incendios de Corrientes, enero–febrero de 2022).

Uso: python pipeline/forestal/validar_cosecha.py
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pyogrio

ROOT = Path(__file__).resolve().parents[2]
MACIZOS = ROOT.parent / 'dndfi_inventario' / 'macizos_23_04_2026.geojson'
COSECHA = ROOT / 'pipeline' / 'output' / 'forestal' / 'cosecha_hansen.parquet'
OUT = Path(__file__).with_name('validacion_cosecha.md')
EDADES = [-1, 5, 10, 15, 20, 25, 30, 200]
ROT = ['0–5', '6–10', '11–15', '16–20', '21–25', '26–30', '> 30']


def main() -> None:
    attrs = pyogrio.read_dataframe(str(MACIZOS), read_geometry=False,
                                   columns=['id', 'grupo_espe', 'superficie', 'estado_pl', 'prov'])
    c = pd.read_parquet(COSECHA).merge(attrs, on='id', how='left')
    c['ha'] = pd.to_numeric(c.superficie, errors='coerce').fillna(0)
    c['edad'] = 2026 - c.anio_plant.astype('float')
    c['clase'] = pd.cut(c.edad, EDADES, labels=ROT)
    lin = [f'# Validación de la marca de pérdida después de plantar (Hansen v1.13)', '',
           f'Generado por `pipeline/forestal/validar_cosecha.py` el {date.today():%d-%m-%Y}. {len(c):,} macizos del NEA; '
           f'marcados (≥ 50 % del macizo con pérdida en un año ≥ plantación + 5, desde 2001): '
           f'{int(c.perdida_post.sum()):,} ({c.loc[c.perdida_post, "ha"].sum():,.0f} ha de {c.ha.sum():,.0f}).', '',
           '## 1. Por edad según el inventario (pinos y eucaliptos)', '',
           '| Grupo | Edad (años) | Macizos | ha | ha marcadas | % de ha |', '|---|---|---:|---:|---:|---:|']
    for g in ('Pinos', 'Eucaliptos'):
        s = c[c.grupo_espe == g]
        for k, d in s.groupby('clase', observed=True):
            hm = d.loc[d.perdida_post, 'ha'].sum()
            lin.append(f'| {g} | {k} | {len(d):,} | {d.ha.sum():,.0f} | {hm:,.0f} | {100 * hm / max(d.ha.sum(), 1):.1f} |')
    lin += ['', '## 2. Por estado declarado en el inventario', '', '| Estado | Macizos | % de ha marcada |', '|---|---:|---:|']
    for e, d in c.groupby(c.estado_pl.fillna('').replace('', '(vacío)')):
        if len(d) < 100:
            continue
        lin.append(f'| {e} | {len(d):,} | {100 * d.loc[d.perdida_post, "ha"].sum() / max(d.ha.sum(), 1):.1f} |')
    inc = c[(c.estado_pl == 'Afectada por incendios') & c.perdida_post]
    lin += ['', '## 3. Año de la pérdida en los macizos «Afectada por incendios» marcados', '',
            '| Año | Macizos |', '|---:|---:|']
    lin += [f'| {int(a)} | {n:,} |' for a, n in inc.anio_perdida.value_counts().sort_index().items() if n >= 20]
    lin += ['', '## 4. Sin píxel', '', f'Macizos sin ningún píxel de 30 m dentro (polígono < 0,1 ha o fuera del ráster): '
            f'{int((c.n_px == 0).sum()):,}. Sin año de plantación (no se marcan): {int(c.anio_plant.isna().sum()):,}.']
    OUT.write_text('\n'.join(lin) + '\n', encoding='utf-8')
    print('\n'.join(lin))


if __name__ == '__main__':
    main()
