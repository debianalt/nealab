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
BANDAS_PAV = [-0.001, 1, 3, 5, 10, np.inf]   # km


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
    # Distancia en línea recta desde el borde del macizo a la ruta pavimentada más cercana (vial.macizos,
    # pipeline/vial/distancia_macizos.py). Bandas: 1 hasta 1 km · 2 1–3 · 3 3–5 · 4 5–10 · 5 más de 10 · 0 sin dato.
    conn = connect(DB)
    dist = pd.read_sql('SELECT id::text AS id_txt, dist_pav_m FROM vial.macizos', conn)
    conn.close()
    df['id_txt'] = df.id.astype(str)
    df = df.merge(dist.drop_duplicates('id_txt'), on='id_txt', how='left')
    km = df.dist_pav_m / 1000
    df['dist_pav_km'] = km.round(1)
    df['banda_pav'] = pd.cut(km, BANDAS_PAV, labels=False).add(1).fillna(0).astype('int8')
    log(f'distancia a pavimento: {int(df.dist_pav_m.notna().sum()):,} de {len(df):,} macizos; '
        f'mediana {km.median():.1f} km')
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
    cols = ['id', 'territorio', 'dpto_key', 'edad', 'clase_edad', 'turno', 'pasado', 'situacion', 'anio_perdida', 'pct_perdida',
            'banda_pav', 'dist_pav_km']
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

    ap_all = pd.to_numeric(df.anio_plant, errors='coerce')
    df['ap'] = np.where(ap_all > 1900, ap_all, np.nan)
    df['anio_turno'] = df.ap + df.turno
    df['gen3'] = np.select([df.genero == 'Pinus', df.genero == 'Eucalyptus'], ['pino', 'euca'], 'otras')

    def columnas(d: pd.DataFrame, cortes: list[tuple[str, pd.Series]]) -> dict:
        # {rotulos, ha: {pino, euca, otras}}: hectáreas por columna (máscara booleana) y género
        ha = {k: [round(float(d.loc[m & (d.gen3 == k), 'ha'].sum())) for _, m in cortes] for k in ('pino', 'euca', 'otras')}
        return {'rotulos': [r for r, _ in cortes], 'ha': ha}

    def series(d: pd.DataFrame) -> dict:
        # Gráficos del panel (CosechaGraficos.svelte); cada columna = el filtro de teselas que pinta al tocarla.
        vivo = d.situacion.isin([1, 2])
        cal = [('hoy', d.situacion.isin([3, 4]))]
        cal += [(str(a), vivo & (d.anio_turno == a)) for a in range(ANIO + 1, ANIO + 16)]
        cal += [(f'{ANIO + 16}+', vivo & (d.anio_turno >= ANIO + 16))]
        pl = [('<1990', d.ap < 1990)] + [(str(a), d.ap == a) for a in range(1990, ANIO)]
        t5 = d.situacion == 5
        ap5 = pd.to_numeric(d.anio_perdida, errors='coerce')
        tal = [(str(a), t5 & (ap5 == a)) for a in range(2001, ANIO)]
        listo = d.situacion.isin([3, 4])
        dis = [(str(b), listo & (d.banda_pav == b)) for b in range(1, len(BANDAS_PAV))]
        return {'calendario': columnas(d, cal), 'plantacion': columnas(d, pl), 'talado': columnas(d, tal),
                'distancia': columnas(d, dis)}

    def chequear(r: dict, s: dict, donde: str) -> None:
        hoy_pino = s['calendario']['ha']['pino'][0]
        assert abs(hoy_pino - r['pino_listo_ha']) <= 1, f'{donde}: calendario hoy pino {hoy_pino} ≠ pino_listo {r["pino_listo_ha"]}'
        tal = sum(sum(v) for v in s['talado']['ha'].values())
        celdas = sum(1 for v in s['talado']['ha'].values() for x in v if x)   # cada celda redondeada ±0,5 ha
        assert abs(tal - r['perdida_ha']) <= 1 + 0.5 * celdas, f'{donde}: talado {tal} ≠ perdida {r["perdida_ha"]}'
        dis = sum(sum(v) for v in s['distancia']['ha'].values())
        hoy = sum(v[0] for v in s['calendario']['ha'].values())   # todo lo listo, todas las especies
        celdas = sum(1 for v in s['distancia']['ha'].values() for x in v if x) + 3
        assert abs(dis - hoy) <= 1 + 0.5 * celdas, f'{donde}: distancia {dis} ≠ listo {hoy} (macizos sin distancia)'

    for terr, g in df.groupby('territorio'):
        base = json.loads((DATA / BASE[terr]).read_text(encoding='utf-8'))
        deps = []
        for dep in base['departments']:
            d = g[g.dpto_key == dep['parquetKey']]
            if d.empty:
                continue
            r = rank(d); se = series(d); chequear(r, se, f"{terr}/{dep['parquetKey']}")
            deps.append({'dpto': dep['dpto'], 'parquetKey': dep['parquetKey'], 'centroid': dep.get('centroid'),
                         'avg_score': r['pino_listo_ha'], 'hex_count': 0, 'rank': r, **se})
        rp, sp = rank(g), series(g); chequear(rp, sp, terr)
        resumen = {'province': {'total_hexes': 0, 'avg_score': None, 'unassigned': int(g.dpto_key.isna().sum()), 'rank': rp, **sp},
                   'departments': deps,
                   'source': 'pipeline/forestal/exportar_cosecha_web.py (DNDFI 23-04-2026, Hansen v1.13, turnos validados 3-nov-2026)'}
        (DATA / RESUMEN[terr]).write_text(json.dumps(resumen, ensure_ascii=False, indent=1), encoding='utf-8')
        log(f'{terr}: {len(g):,} macizos, {len(deps)} deptos con plantaciones; pino listo {resumen["province"]["rank"]["pino_listo_ha"]:,} ha')


if __name__ == '__main__':
    main()
