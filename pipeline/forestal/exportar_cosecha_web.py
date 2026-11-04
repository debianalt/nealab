"""Capa «¿Dónde hay pino y eucalipto para cortar?»: atributos por macizo DNDFI y resúmenes por departamento.

Lee el Inventario Nacional de Plantaciones Forestales (DNDFI, 23-04-2026), la marca de pérdida después de plantar
(`cosecha_hansen.py`) y los turnos validados por el autor (`turnos_forestales.md`, 3-nov-2026). Escribe:

    pipeline/output/forestal/macizos_atributos.parquet   id → edad, clase_edad, situacion, turno, pasado, dpto_key…
    src/lib/data/sat_forestry_harvest_dept_summary.json (Misiones), {t}_sat_forestry_harvest_summary.json (Ctes, Chaco, Fsa)

Después: `python pipeline/build_plantations_tiles.py --territory all --attrs pipeline/output/forestal/macizos_atributos.parquet`
arma `<t>_plantations_v2.pmtiles` con estos atributos.

Clase de edad (clase_edad): 1 0–5 · 2 6–10 · 3 11–15 · 4 16–20 · 5 21–25 · 6 más de 25 · 0 sin año.
Situación (situacion): 1 antes del turno · 2 entra en turno 2026–2030 · 3 en turno · 4 pasado de turno ·
5 pérdida de cobertura después de plantar (Hansen: cosecha o incendio) · 6 sin año o sin turno citado.
Departamento (dpto_key): el de la celda H3 res-9 que contiene el centro del macizo en la partición web
(Hexoma `capa_land_use`, radios INDEC), no el campo `depto` del DNDFI.

Uso (desde neahub/): python pipeline/forestal/exportar_cosecha_web.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import h3
import numpy as np
import pandas as pd
import pyogrio

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

MACIZOS = ROOT.parent / 'dndfi_inventario' / 'macizos_23_04_2026.geojson'
COSECHA = ROOT / 'pipeline' / 'output' / 'forestal' / 'cosecha_hansen.parquet'
OUT = ROOT / 'pipeline' / 'output' / 'forestal'
DATA = ROOT / 'src' / 'lib' / 'data'
ANIO = 2026
PROV = {'MISIONES': 'misiones', 'CORRIENTES': 'corrientes', 'CHACO': 'chaco', 'FORMOSA': 'formosa'}
RESUMEN = {'misiones': 'sat_forestry_harvest_dept_summary.json', 'corrientes': 'corrientes_sat_forestry_harvest_summary.json',
           'chaco': 'chaco_sat_forestry_harvest_summary.json', 'formosa': 'formosa_sat_forestry_harvest_summary.json'}
# nombres y centroides de los departamentos: los de la capa de uso del suelo (misma partición INDEC)
BASE = {'misiones': 'sat_uso_suelo_dept_summary.json', 'corrientes': 'corrientes_sat_uso_suelo_summary.json',
        'chaco': 'chaco_sat_uso_suelo_summary.json', 'formosa': 'formosa_sat_uso_suelo_summary.json'}
# Turnos validados por el autor el 3-nov-2026 (turnos_forestales.md): (en turno desde, pasado de turno desde)
NORTE_MISIONES = {'eldorado', 'san_pedro', 'g_m_belgrano', 'iguazu', 'montecarlo'}   # zona Norte, DNDFI 2024
TURNO = {'Pinus': (16, 23), 'Araucaria': (25, 31), 'Grevillea': (25, None), 'Paulownia': (12, None),
         'Prosopis': (25, None)}
EDADES = [-1, 5, 10, 15, 20, 25, 999]


def turno(genero: str, territorio: str, dpto: str) -> tuple[float, float]:
    if genero == 'Eucalyptus':
        return (7, 12) if territorio == 'misiones' and dpto in NORTE_MISIONES else (12, 17)
    t = TURNO.get(genero)
    return (float(t[0]), float(t[1]) if t[1] is not None else np.inf) if t else (np.nan, np.nan)


def particion(celdas: pd.Series) -> pd.Series:
    conn = connect(DB)
    p = pd.read_sql("""SELECT c.h3::text AS h3, d.parquet_key FROM capa_land_use c JOIN departamentos d USING (depto_id)
                       WHERE c.principal""", conn)
    conn.close()
    return celdas.map(dict(zip(p.h3, p.parquet_key)))


def main() -> None:
    gdf = pyogrio.read_dataframe(str(MACIZOS), columns=['id', 'genero', 'grupo_espe', 'anio_plant', 'superficie', 'prov'],
                                 where="prov IN ('MISIONES','CORRIENTES','CHACO','FORMOSA')")
    log(f'{len(gdf):,} macizos del NEA')
    cen = gdf.geometry.representative_point()
    gdf['h3'] = [h3.latlng_to_cell(p.y, p.x, 9) for p in cen]
    df = pd.DataFrame(gdf.drop(columns='geometry'))
    df['territorio'] = df.prov.map(PROV)
    df['dpto_key'] = particion(df.h3)
    sin = df.dpto_key.isna()
    log(f'sin departamento por celda: {int(sin.sum()):,} macizos ({df.loc[sin, "superficie"].astype(float).sum():,.0f} ha)')
    df['ha'] = pd.to_numeric(df.superficie, errors='coerce').fillna(0.0)
    ap = pd.to_numeric(df.anio_plant, errors='coerce')
    df['edad'] = np.where(ap > 1900, ANIO - ap, np.nan)
    df['clase_edad'] = pd.cut(df.edad, EDADES, labels=False).add(1).fillna(0).astype('int8')
    tt = [turno(g, t, d) for g, t, d in zip(df.genero.fillna(''), df.territorio, df.dpto_key.fillna(''))]
    df['turno'] = [a for a, _ in tt]
    df['pasado'] = [b for _, b in tt]
    c = pd.read_parquet(COSECHA)[['id', 'perdida_post', 'anio_perdida', 'pct_perdida']]
    df = df.merge(c, on='id', how='left')
    df['perdida_post'] = df.perdida_post.fillna(False).astype(bool)
    s = np.full(len(df), 6, np.int8)
    e, t, pz = df.edad.to_numpy(), df.turno.to_numpy(), df.pasado.to_numpy()
    ok = ~np.isnan(e) & ~np.isnan(t)
    s[ok & (e < t - 5)] = 1
    s[ok & (e >= t - 5) & (e < t)] = 2
    s[ok & (e >= t) & (e < pz)] = 3
    s[ok & (e >= pz)] = 4
    s[df.perdida_post.to_numpy()] = 5
    df['situacion'] = s
    OUT.mkdir(parents=True, exist_ok=True)
    cols = ['id', 'territorio', 'dpto_key', 'edad', 'clase_edad', 'turno', 'pasado', 'situacion', 'anio_perdida', 'pct_perdida']
    out = df[cols].copy()
    out['turno'] = out.turno.astype('float32'); out['pasado'] = out.pasado.replace(np.inf, np.nan).astype('float32')
    out.to_parquet(OUT / 'macizos_atributos.parquet', index=False)
    log(f'situación (ha): ' + ', '.join(f'{k}: {v:,.0f}' for k, v in df.groupby('situacion').ha.sum().items()))

    def rank(d: pd.DataFrame) -> dict:
        pin, euc = d[d.genero == 'Pinus'], d[d.genero == 'Eucalyptus']
        listo = lambda x: float(x.loc[x.situacion.isin([3, 4]), 'ha'].sum())  # noqa: E731
        con_edad = d[~d.edad.isna()]
        return {
            'plant_ha': round(float(d.ha.sum())),
            'edad_media': round(float(np.average(con_edad.edad, weights=con_edad.ha)), 1) if con_edad.ha.sum() > 0 else None,
            'pino_listo_ha': round(listo(pin)),
            'pino_listo_pct': round(100 * listo(pin) / pin.ha.sum(), 1) if pin.ha.sum() > 0 else None,
            'pino_entra_ha': round(float(pin.loc[pin.situacion == 2, 'ha'].sum())),
            'euca_listo_ha': round(listo(euc)),
            'euca_listo_pct': round(100 * listo(euc) / euc.ha.sum(), 1) if euc.ha.sum() > 0 else None,
            'perdida_ha': round(float(d.loc[d.situacion == 5, 'ha'].sum())),
            'perdida_pct': round(100 * float(d.loc[d.situacion == 5, 'ha'].sum()) / d.ha.sum(), 1) if d.ha.sum() > 0 else None,
            'macizos': int(len(d)),
        }

    for terr, g in df.groupby('territorio'):
        base = json.loads((DATA / BASE[terr]).read_text(encoding='utf-8'))
        deps = []
        for dep in base['departments']:
            d = g[g.dpto_key == dep['parquetKey']]
            if d.empty:
                continue
            r = rank(d)
            deps.append({'dpto': dep['dpto'], 'parquetKey': dep['parquetKey'], 'centroid': dep.get('centroid'),
                         'avg_score': r['pino_listo_ha'], 'hex_count': 0, 'rank': r})
        resumen = {'province': {'total_hexes': 0, 'avg_score': None, 'unassigned': int(g.dpto_key.isna().sum()), 'rank': rank(g)},
                   'departments': deps,
                   'source': 'pipeline/forestal/exportar_cosecha_web.py (DNDFI 23-04-2026, Hansen v1.13, turnos validados 3-nov-2026)'}
        (DATA / RESUMEN[terr]).write_text(json.dumps(resumen, ensure_ascii=False, indent=1), encoding='utf-8')
        log(f'{terr}: {len(g):,} macizos, {len(deps)} deptos con plantaciones; pino listo {resumen["province"]["rank"]["pino_listo_ha"]:,} ha')


if __name__ == '__main__':
    main()
