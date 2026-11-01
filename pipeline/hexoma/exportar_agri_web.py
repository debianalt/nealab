"""Hexoma → spatia.ar: la capa «potencial agrícola» en unidades físicas y aptitud por cultivo (NEA argentino).

Reemplaza los 6 componentes 0–100 con goalposts de la capa vieja (decisión del autor, 1-nov-2026). Lee del Hexoma
local y escribe los parquets y resúmenes que lee la web, con nombre nuevo (`sat_agri_apt`) para no romper el sitio
publicado entre la subida y el deploy:

    pipeline/output/hexoma/web/{prefijo}sat_dpto/sat_agri_apt_{parquetKey}.parquet   por departamento (misma partición
                                                                                      que la web: capa_agri_potential.depto_id)
    pipeline/output/hexoma/web/{prefijo}sat_agri_apt.parquet                          territorio entero
    src/lib/data/sat_agri_apt_dept_summary.json (Misiones), {t}_sat_agri_apt_summary.json (Corrientes, Chaco, Formosa)

Columnas por celda (h3index):
    p_normal_mm, balance_normal_mm   lluvia CHIRPS y lluvia − ET0, normal 1991–2020 del píxel
    gdd_normal                       grados-día base 10 con temperatura corregida con estaciones, normal
    heladas_normal, heladas5_normal  días/año con Tmin < 0 °C en abrigo y a 5 cm (corregidas), normal
    calor35_normal                   días/año con Tmax > 35 °C (corregida), normal de la campaña
    ph, arcilla_pct, pendiente_pct, elev_m   SoilGrids 0–30 cm y FABDEM
    apt_<c>_clase                    clase mediana 1–4 (S1, S2, S3, N) de 1991/92–2024/25 (aptitud.resumen)
    apt_<c>_pct                      % de campañas S1+S2
Resumen por departamento: rank.<c>_ha (ha con clase S1–S2) y rank.<c>_pct (% de la superficie).

Uso (desde neahub/):
    python pipeline/hexoma/exportar_agri_web.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from aptitud_reglas import NOMBRES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'pipeline' / 'output' / 'hexoma' / 'web'
DATA = ROOT / 'src' / 'lib' / 'data'
PREFIJO = {'misiones': '', 'corrientes': 'corrientes/', 'chaco': 'chaco/', 'formosa': 'formosa/'}
RESUMEN = {'misiones': 'sat_agri_apt_dept_summary.json', 'corrientes': 'corrientes_sat_agri_apt_summary.json',
           'chaco': 'chaco_sat_agri_apt_summary.json', 'formosa': 'formosa_sat_agri_apt_summary.json'}
VIEJO = {'misiones': 'sat_agri_potential_dept_summary.json', 'corrientes': 'corrientes_sat_agri_potential_summary.json',
         'chaco': 'chaco_sat_agri_potential_summary.json', 'formosa': 'formosa_sat_agri_potential_summary.json'}
CULTIVOS = list(NOMBRES)


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    apt = ', '.join(f'r.{c}_clase AS apt_{c}_clase, r.{c}_s12_pct AS apt_{c}_pct' for c in CULTIVOS)
    q = f"""
    SELECT a.h3::text AS h3index, t.territorio, d.parquet_key, h3_cell_area(a.h3, 'km^2') * 100 AS ha,
           n_p.media::real AS p_normal_mm, n_b.media::real AS balance_normal_mm, n_g.media::real AS gdd_normal,
           n_h.media::real AS heladas_normal, n_hs.media::real AS heladas5_normal, n_c.media::real AS calor35_normal,
           s.ph_h2o::real AS ph, s.arcilla_pct::real AS arcilla_pct,
           (100 * tan(radians(s.pendiente_media_grados)))::real AS pendiente_pct, s.elev_media_m::real AS elev_m,
           {apt}
    FROM capa_agri_potential a
    JOIN departamentos d USING (depto_id) JOIN territorios t ON t.territorio_id = a.territorio_id
    JOIN clima.hex_pixel h ON h.h3 = a.h3
    LEFT JOIN suelo.h3 s ON s.h3 = a.h3
    LEFT JOIN aptitud.resumen r ON r.h3 = a.h3
    LEFT JOIN clima.normal_1991_2020 n_p ON n_p.tabla = 'chirps_campania' AND n_p.variable = 'p_mm' AND n_p.unidad_id = h.chirps_pixel AND n_p.estacion IS NULL
    LEFT JOIN clima.normal_1991_2020 n_b ON n_b.tabla = 'balance_campania' AND n_b.variable = 'p_menos_et0' AND n_b.unidad_id = h.par_id AND n_b.estacion IS NULL
    LEFT JOIN clima.normal_1991_2020 n_g ON n_g.tabla = 'era5_campania_corr' AND n_g.variable = 'gdd10' AND n_g.unidad_id = h.era5_pixel AND n_g.estacion IS NULL
    LEFT JOIN clima.normal_1991_2020 n_h ON n_h.tabla = 'era5_helada_corr' AND n_h.variable = 'dias_tmin_lt0' AND n_h.unidad_id = h.era5_pixel
    LEFT JOIN clima.normal_1991_2020 n_hs ON n_hs.tabla = 'era5_helada_corr' AND n_hs.variable = 'dias_helada_suelo' AND n_hs.unidad_id = h.era5_pixel
    LEFT JOIN clima.normal_1991_2020 n_c ON n_c.tabla = 'era5_campania_corr' AND n_c.variable = 'dias_tmax35' AND n_c.unidad_id = h.era5_pixel AND n_c.estacion IS NULL
    WHERE a.principal"""
    df = pd.read_sql(q, conn)
    log(f'{len(df):,} celdas; sin aptitud: {df.apt_soja_clase.isna().sum():,}; sin lluvia: {df.p_normal_mm.isna().sum():,}')
    for c in CULTIVOS:
        df[f'apt_{c}_clase'] = df[f'apt_{c}_clase'].astype('Int8')
        df[f'apt_{c}_pct'] = df[f'apt_{c}_pct'].astype('Int16')
    cols = [c for c in df.columns if c not in ('territorio', 'parquet_key', 'ha')]
    for terr, g in df.groupby('territorio'):
        pre = PREFIJO[terr]
        (OUT / pre / 'sat_dpto').mkdir(parents=True, exist_ok=True)
        g.sort_values('h3index')[cols].to_parquet(OUT / pre / 'sat_agri_apt.parquet', index=False,
                                                    compression='zstd', row_group_size=50_000)
        for key, d in g.groupby('parquet_key'):
            d.sort_values('h3index')[cols].to_parquet(OUT / pre / 'sat_dpto' / f'sat_agri_apt_{key}.parquet',
                                                       index=False, compression='zstd')
        # resumen: parte del viejo (nombres, centroides) y reemplaza los valores
        viejo = json.loads((DATA / VIEJO[terr]).read_text(encoding='utf-8'))
        por_key = {k: d for k, d in g.groupby('parquet_key')}
        deps = []
        for dep in viejo['departments']:
            d = por_key.get(dep['parquetKey'])
            if d is None:
                continue
            rank = {}
            for c in CULTIVOS:
                ok = d[f'apt_{c}_clase'].notna()
                apto = ok & (d[f'apt_{c}_clase'] <= 2)
                rank[f'{c}_ha'] = round(float(d.loc[apto, 'ha'].sum()))
                rank[f'{c}_pct'] = round(100 * float(apto.sum()) / max(int(ok.sum()), 1), 1)
            nuevo = {k: v for k, v in dep.items() if k not in ('avg_score', 'rank')}
            nuevo['hex_count'] = int(len(d))
            nuevo['avg_score'] = round(float(d.apt_soja_pct.mean()), 1)  # sólo ordena si faltara el ranking
            nuevo['rank'] = rank
            deps.append(nuevo)
        resumen = {k: v for k, v in viejo.items() if k not in ('departments', 'province')}
        resumen['province'] = {'total_hexes': int(len(g)), 'avg_score': round(float(g.apt_soja_pct.mean()), 1), 'unassigned': 0}
        resumen['departments'] = deps
        resumen['source'] = 'pipeline/hexoma/exportar_agri_web.py (Hexoma: clima corregido, SoilGrids, FABDEM, aptitud A4)'
        (DATA / RESUMEN[terr]).write_text(json.dumps(resumen, ensure_ascii=False, indent=1), encoding='utf-8')
        mb = sum(f.stat().st_size for f in (OUT / pre / 'sat_dpto').glob('sat_agri_apt_*.parquet')) / 1e6
        log(f'{terr}: {len(g):,} celdas, {g.parquet_key.nunique()} deptos, {mb:.1f} MB por depto + '
            f'{(OUT / pre / "sat_agri_apt.parquet").stat().st_size / 1e6:.1f} MB global; resumen {RESUMEN[terr]}')
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
