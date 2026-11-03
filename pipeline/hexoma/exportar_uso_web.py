"""Hexoma → spatia.ar: la capa «uso del suelo» con MapBiomas Argentina Colección 3 (NEA argentino).

Reemplaza Dynamic World 2024 (decisión del autor, 3-nov-2026): en Misiones «árboles» juntaba monte, pino y buena parte
de yerba y té. Lee la serie anual por clase del Hexoma (`%HEXOMA_FUENTES%/uso/uso_h3_anio_clase.parquet`, fase A2) y la
partición por departamento de la capa web (`capa_land_use.depto_id`, filas `principal`), y escribe con nombre nuevo
(`sat_uso_suelo`) para no romper el sitio publicado entre la subida y el deploy:

    pipeline/output/hexoma/web/{prefijo}sat_dpto/sat_uso_suelo_{parquetKey}.parquet   por departamento
    pipeline/output/hexoma/web/{prefijo}sat_uso_suelo.parquet                          territorio entero
    src/lib/data/sat_uso_suelo_dept_summary.json (Misiones), {t}_sat_uso_suelo_summary.json (Corrientes, Chaco, Formosa)

Clases (código entero, nominal; MapBiomas → Spatia):
    1 monte nativo (3, 4, 6) · 2 plantación forestal (9) · 3 yerba, té y otros perennes (36) · 4 cultivo anual (19, 21)
    5 pastura implantada (15) · 6 pastizal y arbustal (12, 63, 66, 77) · 7 estero y pastizal inundable (11, 73)
    8 urbano (24) · 9 agua (33) · 10 otro sin vegetación (25, 27, 34) · 11 mosaico (ninguna clase llega al 50 %)
Trayectoria 1985 → 2025 sobre las clases dominantes:
    1 monte que sigue · 2 monte → plantación · 3 monte → perenne · 4 monte → uso agropecuario o mosaico (4, 5, 6, 11)
    5 monte → urbano o agua · 6 volvió el monte · 7 otro cambio · 8 sin monte, sin cambio

Columnas por celda (h3index): dom_2025, dom_1985, tray (Int8); pct_<clase> 2025 (Int16, % de la superficie observada).
Resumen por departamento y provincia: rank.* (ver RANK) y serie = ha por clase cada 5 años, 1985–2025.

Uso (desde neahub/):
    python pipeline/hexoma/exportar_uso_web.py [--evaluar]   (--evaluar: sólo la tabla de umbrales, sin escribir)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'pipeline' / 'output' / 'hexoma' / 'web'
DATA = ROOT / 'src' / 'lib' / 'data'
FUENTES = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
SERIE_CLASES = FUENTES / 'uso' / 'uso_h3_anio_clase.parquet'
PREFIJO = {'misiones': '', 'corrientes': 'corrientes/', 'chaco': 'chaco/', 'formosa': 'formosa/'}
RESUMEN = {'misiones': 'sat_uso_suelo_dept_summary.json', 'corrientes': 'corrientes_sat_uso_suelo_summary.json',
           'chaco': 'chaco_sat_uso_suelo_summary.json', 'formosa': 'formosa_sat_uso_suelo_summary.json'}
# Resumen viejo (Dynamic World): sólo para nombres y centroides en la primera corrida.
RESUMEN_DW = {'misiones': 'sat_land_use_dept_summary.json', 'corrientes': 'corrientes_sat_land_use_summary.json',
              'chaco': 'chaco_sat_land_use_summary.json', 'formosa': 'formosa_sat_land_use_summary.json'}

CLASES = ['monte', 'plantacion', 'perenne', 'cultivo', 'pastura', 'pastizal', 'estero', 'urbano', 'agua', 'otro']
MAPBIOMAS = {3: 'monte', 4: 'monte', 6: 'monte', 9: 'plantacion', 36: 'perenne', 19: 'cultivo', 21: 'cultivo',
             15: 'pastura', 12: 'pastizal', 63: 'pastizal', 66: 'pastizal', 77: 'pastizal', 11: 'estero', 73: 'estero',
             24: 'urbano', 33: 'agua', 25: 'otro', 27: 'otro', 34: 'otro'}
MOSAICO = 11
UMBRAL = 0.5
ANIOS = list(range(1985, 2026, 5))


def dominante(f: np.ndarray, umbral: float = UMBRAL) -> np.ndarray:
    """Código 1–10 de la clase con ≥ umbral de la superficie, 11 si ninguna llega; 0 sin observación."""
    tiene = f.sum(1) > 0
    dom = np.where(f.max(1) >= umbral, f.argmax(1) + 1, MOSAICO)
    return np.where(tiene, dom, 0).astype(np.int8)


def trayectoria(d85: np.ndarray, d25: np.ndarray) -> np.ndarray:
    m85, m25 = d85 == 1, d25 == 1
    t = np.full(len(d85), 8, np.int8)
    t[(d85 != d25)] = 7
    t[~m85 & m25] = 6
    t[m85 & np.isin(d25, [8, 9])] = 5
    t[m85 & np.isin(d25, [4, 5, 6, 11])] = 4
    t[m85 & (d25 == 3)] = 3
    t[m85 & (d25 == 2)] = 2
    t[m85 & m25] = 1
    t[(d85 == 0) | (d25 == 0)] = 0
    return t


def leer() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Celdas principales de la capa web con partición, superficie y fracciones 1985 y 2025; y la serie de ha por
    (departamento, año, clase), agregada en DuckDB sin pasar por una tabla ancha de 3,3 M × 90."""
    conn = connect(DB)
    part = pd.read_sql("""
        SELECT c.h3::text AS h3, t.territorio, d.parquet_key, d.departamento, h3_cell_area(c.h3, 'km^2') * 100 AS ha
        FROM capa_land_use c JOIN departamentos d USING (depto_id) JOIN territorios t ON t.territorio_id = c.territorio_id
        WHERE c.principal""", conn)
    conn.close()
    log(f'partición web: {len(part):,} celdas, {part.parquet_key.nunique()} deptos')
    con = duckdb.connect()
    con.register('part', part[['h3', 'territorio', 'parquet_key', 'ha']])
    caso = ' '.join(f'WHEN {k} THEN {CLASES.index(v)}' for k, v in MAPBIOMAS.items())
    con.execute(f"""
        CREATE TEMP TABLE px AS
        SELECT s.h3, s.anio, CASE s.clase {caso} END AS k, sum(s.n_px)::DOUBLE AS n
        FROM '{SERIE_CLASES.as_posix()}' s SEMI JOIN part USING (h3)
        WHERE s.anio IN ({','.join(map(str, ANIOS))}) AND s.clase IN ({','.join(map(str, MAPBIOMAS))})
        GROUP BY ALL""")
    con.execute('CREATE TEMP TABLE fr AS SELECT h3, anio, k, n / sum(n) OVER (PARTITION BY h3, anio) AS f FROM px')
    cols = ', '.join(f"coalesce(sum(f) FILTER (WHERE anio = {a} AND k = {i}), 0)::FLOAT AS f{a}_{c}"
                     for a in (1985, 2025) for i, c in enumerate(CLASES))
    ancho = con.execute(f'SELECT h3, {cols} FROM fr WHERE anio IN (1985, 2025) GROUP BY h3').df()
    serie = con.execute("""
        SELECT p.territorio, p.parquet_key, fr.anio, fr.k, sum(fr.f * p.ha) AS ha
        FROM fr JOIN part p USING (h3) GROUP BY ALL""").df()
    con.close()
    df = part.merge(ancho, on='h3', how='left')
    fcols = [c for c in df.columns if c.startswith('f19') or c.startswith('f20')]
    df[fcols] = df[fcols].fillna(0.0)
    log(f'fracciones 1985/2025: {len(ancho):,} celdas con observación; serie: {len(serie):,} filas')
    return df, serie


def fracciones(df: pd.DataFrame, a: int) -> np.ndarray:
    return df[[f'f{a}_{c}' for c in CLASES]].to_numpy()


def evaluar(df: pd.DataFrame) -> str:
    """Tabla del umbral de mosaico con las 11 clases finales, por provincia y por departamento de Misiones."""
    f = fracciones(df, 2025)
    mx = f.max(1)
    filas = ['| Provincia | celdas | sin clase ≥40 % | ≥50 % | ≥60 % |', '|---|---:|---:|---:|---:|']
    for terr, g in df.groupby('territorio', sort=False):
        m = mx[g.index]
        filas.append(f'| {terr} | {len(g):,} | ' + ' | '.join(f'{100 * (m < u).mean():.1f} %' for u in (.4, .5, .6)) + ' |')
    mis = df.territorio == 'misiones'
    filas += ['', '| Departamento de Misiones | mosaico al 50 % |', '|---|---:|']
    por = pd.Series(mx[mis] < UMBRAL, index=df.loc[mis, 'departamento']).groupby(level=0).mean().sort_values(ascending=False)
    filas += [f'| {k} | {100 * v:.1f} % |' for k, v in por.items()]
    return '\n'.join(filas)


def rank_de(d: pd.DataFrame) -> dict:
    ha = d['ha'].to_numpy()
    f25, f85 = fracciones(d, 2025), fracciones(d, 1985)
    sup = float(ha[f25.sum(1) > 0].sum()) or 1.0
    sup85 = float(ha[f85.sum(1) > 0].sum()) or 1.0
    i = {c: CLASES.index(c) for c in CLASES}
    area = lambda f, c: float((f[:, i[c]] * ha).sum())  # noqa: E731
    monte85 = area(f85, 'monte')
    # Saldo neto del departamento (> 0 = se perdió). No la suma de las bajas de cada celda: ésa suma también el ruido
    # de clasificación entre años (Misiones: 474 mil ha brutas contra 330 mil netas, 1985–2025).
    perdido = monte85 - area(f25, 'monte')
    agro = area(f25, 'pastura') + area(f25, 'cultivo')
    return {
        'monte_pct': round(100 * area(f25, 'monte') / sup, 1), 'monte_ha': round(area(f25, 'monte')),
        'monte85_pct': round(100 * monte85 / sup85, 1), 'monte85_ha': round(monte85),
        'plantacion_ha': round(area(f25, 'plantacion')), 'plantacion_pct': round(100 * area(f25, 'plantacion') / sup, 1),
        'perenne_pct': round(100 * area(f25, 'perenne') / sup, 1),
        'agro_ha': round(agro), 'agro_pct': round(100 * agro / sup, 1),
        'monte_perdido_ha': round(perdido), 'monte_perdido_pct': round(100 * perdido / monte85, 1) if monte85 > 0 else None,
    }


def serie_de(s: pd.DataFrame) -> dict:
    """ha por clase cada 5 años de las filas de la serie agregada (un depto o la provincia entera)."""
    t = s.groupby(['k', 'anio']).ha.sum()
    return {'anios': ANIOS,
            'ha': {c: [round(float(t.get((i, a), 0.0))) for a in ANIOS] for i, c in enumerate(CLASES)}}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--evaluar', action='store_true')
    args = ap.parse_args()
    t0 = time.time()
    df, serie = leer()
    tabla = evaluar(df)
    print(tabla)
    if args.evaluar:
        return
    d25, d85 = dominante(fracciones(df, 2025)), dominante(fracciones(df, 1985))
    out = pd.DataFrame({'h3index': df.h3, 'dom_2025': d25, 'dom_1985': d85, 'tray': trayectoria(d85, d25)})
    for c in CLASES:
        out[f'pct_{c}'] = np.round(100 * df[f'f2025_{c}']).astype('int16')
    sin = d25 == 0
    for c in ['dom_2025', 'dom_1985', 'tray'] + [f'pct_{c}' for c in CLASES]:
        out[c] = out[c].astype('Int16' if c.startswith('pct') else 'Int8')
        out.loc[sin, c] = pd.NA
    log(f'{len(out):,} celdas; sin observación 2025: {int(sin.sum()):,}')
    for terr, g in df.groupby('territorio'):
        pre = PREFIJO[terr]
        (OUT / pre / 'sat_dpto').mkdir(parents=True, exist_ok=True)
        o = out.loc[g.index]
        o.sort_values('h3index').to_parquet(OUT / pre / 'sat_uso_suelo.parquet', index=False,
                                            compression='zstd', row_group_size=50_000)
        for key, idx in g.groupby('parquet_key').groups.items():
            o.loc[idx].sort_values('h3index').to_parquet(OUT / pre / 'sat_dpto' / f'sat_uso_suelo_{key}.parquet',
                                                         index=False, compression='zstd')
        base_p = DATA / RESUMEN[terr]
        base = json.loads((base_p if base_p.exists() else DATA / RESUMEN_DW[terr]).read_text(encoding='utf-8'))
        meta = {dep['parquetKey']: dep for dep in base['departments']}
        deps = []
        for key, d in g.groupby('parquet_key'):
            viejo = meta.get(key, {})
            rank = rank_de(d)
            deps.append({'dpto': viejo.get('dpto', d.departamento.iloc[0]), 'parquetKey': key,
                         'avg_score': rank['monte_pct'], 'hex_count': int(len(d)),
                         **({'centroid': viejo['centroid']} if 'centroid' in viejo else {}),
                         'rank': rank, 'serie': serie_de(serie[(serie.territorio == terr) & (serie.parquet_key == key)])})
        deps.sort(key=lambda x: x['dpto'])
        resumen = {'province': {'total_hexes': int(len(g)), 'avg_score': rank_de(g)['monte_pct'], 'unassigned': 0,
                                'rank': rank_de(g), 'serie': serie_de(serie[serie.territorio == terr])},
                   'departments': deps,
                   'source': 'pipeline/hexoma/exportar_uso_web.py (Hexoma: MapBiomas Argentina Colección 3, 1985–2025)'}
        base_p.write_text(json.dumps(resumen, ensure_ascii=False, indent=1), encoding='utf-8')
        mb = sum(f.stat().st_size for f in (OUT / pre / 'sat_dpto').glob('sat_uso_suelo_*.parquet')) / 1e6
        log(f'{terr}: {len(g):,} celdas, {g.parquet_key.nunique()} deptos, {mb:.1f} MB por depto + '
            f'{(OUT / pre / "sat_uso_suelo.parquet").stat().st_size / 1e6:.1f} MB global; resumen {RESUMEN[terr]}')
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
