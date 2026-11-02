"""Re-split the satellite layers of Corrientes, Chaco and Formosa by INDEC departments.

Two partitions coexisted (measured 2-nov-2026): Misiones and every census layer were split
by INDEC census radios; the satellite layers of these three provinces by GADM departments
(h3_admin_crosswalk.parquet, from ARG_adm2.shp). The department line, the clip and the
rankings could not agree with both. This moves each cell to the INDEC department whose
radios contain its centre (the rule of the web's hole-fill and of the ?h3= index). Cell
values do not depend on the department: nothing is recomputed from rasters, only which
department file a cell lives in and the per-department numbers of the summaries.

Steps (each territory):
  1. crosswalk   output/<t>/h3_admin_crosswalk.parquet rebuilt over the same cells
                 (old one kept as h3_admin_crosswalk.gadm.parquet). Cells whose centre is
                 outside the province's radios (river and border slivers, 0.4-0.7 %) keep
                 their GADM department, so no cell loses its value.
  2. families    the department files R2 serves (truth: CI refreshes flood weekly) are
                 downloaded with a byte backup, concatenated, re-assigned and rewritten
                 under the same keys; Chaco gains «2 de Abril» (GADM had 24 departments).
  3. summaries   src/lib/data/<t>_*_summary.json: dpto = INDEC name, parquetKey kept,
                 hex_count, centroid and avg_score (same column as before, found by
                 matching the published values) recomputed; agri `rank` recomputed with
                 exportar_agri_web.py's rule. The other `rank` blocks are rebuilt after
                 upload by build_layer_ranks / build_deforest_rank / build_flood_rank /
                 build_eudr_dpto, which read the files the CDN serves.

    python pipeline/repartir_deptos_indec.py                   # dry: report only
    python pipeline/repartir_deptos_indec.py --write           # write crosswalk, files, summaries
    python pipeline/repartir_deptos_indec.py --write --upload  # and upload the department files
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
import sys
from pathlib import Path

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / 'pipeline' / 'output'
DATA = REPO / 'src' / 'lib' / 'data'
BACKUP = Path('C:/Users/ant/r2-dept-gadm-backup-2026-11')
TERRITORIES = ['corrientes', 'chaco', 'formosa']
# family: (R2 folder, file stem, summary JSON suffix)
FAMILIES = {
    'flood_risk':             ('flood_dpto', 'hex_flood', 'flood_dept_summary'),
    'accessibility':          ('sat_dpto', 'sat_accessibility', 'sat_accessibility_summary'),
    'agri_potential':         ('sat_dpto', 'sat_agri_apt', 'sat_agri_apt_summary'),
    'deforestation_dynamics': ('sat_dpto', 'sat_deforestation_dynamics', 'sat_deforestation_dynamics_summary'),
    'eudr':                   ('sat_dpto', 'sat_eudr', 'sat_eudr_summary'),
    'forestry_aptitude':      ('sat_dpto', 'sat_forestry_aptitude', 'sat_forestry_aptitude_summary'),
    'land_use':               ('sat_dpto', 'sat_land_use', 'sat_land_use_summary'),
    'pm25_drivers':           ('sat_dpto', 'sat_pm25_drivers', 'sat_pm25_drivers_summary'),
}
AGRI_CROPS_PREFIX = 'apt_'


def log(*a):
    print(*a, flush=True)


def dept_names() -> dict[str, str]:
    src = (REPO / 'pipeline/build_censo_temporal_dept.py').read_text(encoding='utf-8')
    for n in ast.parse(src).body:
        tg = n.targets[0] if isinstance(n, ast.Assign) else getattr(n, 'target', None)
        if getattr(tg, 'id', '') == 'DEPT_NAMES':
            return ast.literal_eval(n.value)
    raise SystemExit('DEPT_NAMES not found')


def safe_filename(name: str) -> str:  # split_by_admin.safe_filename
    return (name.lower().replace(' ', '_')
            .replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
            .replace('ñ', 'n').replace('ü', 'u').replace("'", '').replace('.', ''))


def s3_client():
    import boto3
    env = {}
    for line in open(os.path.expanduser('~/Documents/secrets/cloudfare1.env'), encoding='utf-8'):
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            env[k] = v.strip().strip('"')
    return boto3.client('s3', endpoint_url=env['R2_S3_ENDPOINT'], aws_access_key_id=env['R2_ACCESS_KEY_ID'],
                        aws_secret_access_key=env['R2_SECRET_ACCESS_KEY'], region_name='auto')


# ── 1. crosswalk ────────────────────────────────────────────────────────────────

def indec_crosswalk(t: str, names: dict[str, str]) -> tuple[pd.DataFrame, dict[str, str]]:
    """old cells → INDEC dept code (centre in radios). Returns (df[h3index, gadm, code], gadm name → code)."""
    old = pd.read_parquet(OUT / t / 'h3_admin_crosswalk.gadm.parquet'
                          if (OUT / t / 'h3_admin_crosswalk.gadm.parquet').exists()
                          else OUT / t / 'h3_admin_crosswalk.parquet')
    old = old.rename(columns={'dpto': 'gadm'})
    r = gpd.read_parquet(OUT / t / f'radios_{t}.parquet')[['redcode', 'geometry']].to_crs(4326)
    r['code'] = r['redcode'].astype(str).str[:5]
    r['geometry'] = shapely.make_valid(r.geometry.values)
    ll = np.array([h3.cell_to_latlng(c) for c in old.h3index])
    pts = gpd.GeoDataFrame(old, geometry=gpd.points_from_xy(ll[:, 1], ll[:, 0]), crs=4326)
    j = gpd.sjoin(pts, r[['code', 'geometry']], how='left', predicate='within')
    j = j[~j.index.duplicated()]  # a centre on a shared radio edge
    old['code'] = j['code'].values
    # GADM name → INDEC code by majority (the 58 GADM departments map one to one)
    g2c = old.dropna(subset=['code']).groupby('gadm')['code'].agg(lambda s: s.value_counts().index[0]).to_dict()
    if len(set(g2c.values())) != len(g2c):
        raise SystemExit(f'{t}: two GADM departments map to one INDEC department: {g2c}')
    outside = old['code'].isna()
    old.loc[outside, 'code'] = old.loc[outside, 'gadm'].map(g2c)
    missing = sorted(set(c for c in names if c[:2] == {'corrientes': '18', 'chaco': '22', 'formosa': '34'}[t])
                     - set(old['code']))
    if missing:
        raise SystemExit(f'{t}: INDEC departments with no cell: {missing}')
    moved = (old['code'] != old['gadm'].map(g2c)).sum()
    log(f'{t}: {len(old):,} cells, {moved:,} change department ({100 * moved / len(old):.1f} %), '
        f'{int(outside.sum()):,} centres outside the radios keep GADM; '
        f'{old.code.nunique()} INDEC departments (GADM {old.gadm.nunique()})')
    return old[['h3index', 'gadm', 'code']], g2c


# ── 2. families ─────────────────────────────────────────────────────────────────

def fetch_family(s3, t: str, folder: str, stem: str) -> dict[str, Path]:
    """Download the served department files (byte backup kept). key → local path."""
    prefix = f'data/{t}/{folder}/{stem}_'
    keys = []
    for pg in s3.get_paginator('list_objects_v2').paginate(Bucket='neahub', Prefix=prefix):
        keys += [o['Key'] for o in pg.get('Contents', [])]
    out = {}
    for k in keys:
        dst = BACKUP / k
        if not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file('neahub', k, str(dst))
        out[k[len(prefix):-len('.parquet')]] = dst
    return out


def metric_column(df: pd.DataFrame, by: pd.Series, published: dict[str, float]) -> str | None:
    """The column whose department mean reproduces the published avg_score (rounded to 0.1)."""
    best, best_hits = None, 0
    for c in df.columns:
        if c == 'h3index' or not pd.api.types.is_numeric_dtype(df[c]):
            continue
        m = df[c].groupby(by).mean().round(1)
        hits = sum(1 for k, v in published.items() if v is not None and k in m and abs(m[k] - v) < 0.051)
        if hits > best_hits:
            best, best_hits = c, hits
    n = sum(1 for v in published.values() if v is not None)
    return best if n and best_hits >= max(1, int(0.8 * n)) else None


def agri_rank(d: pd.DataFrame) -> dict:
    """exportar_agri_web.py: ha with class S1–S2 and % of the cells with a class."""
    ha = np.array([h3.cell_area(c, 'km^2') * 100 for c in d.h3index])
    rank = {}
    for col in [c for c in d.columns if c.startswith(AGRI_CROPS_PREFIX) and c.endswith('_clase')]:
        crop = col[len(AGRI_CROPS_PREFIX):-len('_clase')]
        ok = d[col].notna().to_numpy()
        apto = ok & (d[col].fillna(9).to_numpy() <= 2)
        rank[f'{crop}_ha'] = round(float(ha[apto].sum()))
        rank[f'{crop}_pct'] = round(100 * float(apto.sum()) / max(int(ok.sum()), 1), 1)
    return rank


def resplit(s3, t: str, family: str, xw: pd.DataFrame, names: dict[str, str], write: bool) -> list[tuple[Path, str]]:
    folder, stem, summ = FAMILIES[family]
    files = fetch_family(s3, t, folder, stem)
    summary_path = DATA / f'{t}_{summ}.json'
    summary = json.loads(summary_path.read_text(encoding='utf-8'))
    deps = summary['departments']
    key_of_code: dict[str, str] = {}
    parts, schema = [], None
    listed = {d['parquetKey'] for d in deps}
    # files the summary does not list (a Misiones department leaked into Corrientes
    # forestry) go last, so their duplicated cells yield to the territory's own file
    for key, p in sorted(files.items(), key=lambda kv: kv[0] not in listed):
        tb = pq.read_table(p)
        schema = schema or tb.schema
        df = tb.to_pandas()
        df['_old'] = key
        parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    dup = df.h3index.duplicated().sum()
    df = df.drop_duplicates('h3index')
    code = df.h3index.map(xw.set_index('h3index')['code'])
    # cells not in the crosswalk keep the department of their file
    old_code = code[code.notna()].groupby(df.loc[code.notna(), '_old']).agg(lambda s: s.value_counts().index[0])
    nox = code.isna()
    code[nox] = df.loc[nox, '_old'].map(old_code)
    df['_code'] = code
    for k, c in old_code.items():
        key_of_code.setdefault(c, k)
    for c in sorted(set(df['_code'].dropna())):
        key_of_code.setdefault(c, safe_filename(names[c]))
    moved = (df['_code'] != df['_old'].map(old_code)).sum()
    published = {d['parquetKey']: d.get('avg_score') for d in deps}
    # flood: split_flood_by_dpto.py's columns (the weekly radar refresh moved the means since)
    metric = 'flood_risk_score' if family == 'flood_risk' else         metric_column(df.drop(columns=['_old', '_code']), df['_old'], published)
    log(f'  {t}/{family}: {len(files)} files, {len(df):,} cells (dup {dup}, not in crosswalk {int(nox.sum())}), '
        f'{moved:,} move; avg_score = {metric}')

    by_key = {d['parquetKey']: d for d in deps}
    new_deps, staged = [], []
    cols = [f.name for f in schema]
    for c, g in df.groupby('_code'):
        key = key_of_code[c]
        g = g.sort_values('h3index')
        dst = OUT / t / 'indec_resplit' / folder / f'{stem}_{key}.parquet'
        if write:
            dst.parent.mkdir(parents=True, exist_ok=True)
            tb = pa.Table.from_pandas(g[cols], preserve_index=False).cast(schema)
            pq.write_table(tb, dst, compression='zstd', compression_level=9)
            staged.append((dst, f'data/{t}/{folder}/{stem}_{key}.parquet'))
        ll = np.array([h3.cell_to_latlng(x) for x in g.h3index])
        prev = dict(by_key.get(key, {}))
        dep = {k: v for k, v in prev.items() if k not in ('dpto', 'parquetKey', 'hex_count', 'centroid', 'avg_score')}
        new = {'dpto': names[c], 'parquetKey': key,
               'avg_score': (None if metric is None or g[metric].isna().all()
                             else round(float(g[metric].mean()), 1)) if metric else prev.get('avg_score'),
               'hex_count': int(len(g)),
               'centroid': [round(float(ll[:, 1].mean()), 4), round(float(ll[:, 0].mean()), 4)]}
        new.update(dep)
        if family == 'flood_risk':
            new['high_risk_count'] = int((g['jrc_occurrence'] > 10).sum())
        if family == 'agri_potential':
            new['rank'] = agri_rank(g)
        new_deps.append(new)
    if 'province' in summary:
        summary['province']['total_hexes'] = int(len(df))
    summary['departments'] = sorted(new_deps, key=lambda d: d.get('avg_score') or 0, reverse=True)
    summary['partition'] = 'INDEC 2022 radios (pipeline/repartir_deptos_indec.py, 2-nov-2026)'
    if write:
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return staged


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--upload', action='store_true')
    ap.add_argument('--upload-only', action='store_true', help='upload what --write left in output/<t>/indec_resplit')
    ap.add_argument('--territory', choices=TERRITORIES)
    ap.add_argument('--family', choices=list(FAMILIES))
    a = ap.parse_args()
    names = dept_names()
    s3 = s3_client()
    staged: list[tuple[Path, str]] = []
    for t in [a.territory] if a.territory else TERRITORIES:
        if a.upload_only:
            for p in sorted((OUT / t / 'indec_resplit').rglob('*.parquet')):
                staged.append((p, f'data/{t}/{p.parent.name}/{p.name}'))
            continue
        xw, _ = indec_crosswalk(t, names)
        if a.write:
            gadm = OUT / t / 'h3_admin_crosswalk.gadm.parquet'
            if not gadm.exists():
                shutil.copy2(OUT / t / 'h3_admin_crosswalk.parquet', gadm)
            pd.DataFrame({'h3index': xw.h3index, 'dpto': xw.code.map(names)}).to_parquet(
                OUT / t / 'h3_admin_crosswalk.parquet', index=False)
        for fam in [a.family] if a.family else FAMILIES:
            staged += resplit(s3, t, fam, xw, names, a.write)
    if a.upload or a.upload_only:
        from concurrent.futures import ThreadPoolExecutor
        def put(item):
            p, k = item
            # flood_dpto is refreshed in place by the weekly CI (update_flood_current.SHORT_CACHE);
            # the rest sit under a ?v URL (config.ts DEPT_V, bumped with this re-split)
            # (served sat_dpto objects carry no Cache-Control: the CDN rule decides)
            extra = {'ContentType': 'application/vnd.apache.parquet'}
            if '/flood_dpto/' in k:
                extra['CacheControl'] = 'public, max-age=3600'
            s3.upload_file(str(p), 'neahub', k, ExtraArgs=extra)
            head = s3.head_object(Bucket='neahub', Key=k)
            if head['ContentLength'] != p.stat().st_size:
                raise RuntimeError(f'{k}: R2 serves {head["ContentLength"]} B, uploaded {p.stat().st_size} B')
            return k
        with ThreadPoolExecutor(16) as ex:
            done = list(ex.map(put, staged))
        log(f'uploaded {len(done)} files')
    return 0


if __name__ == '__main__':
    sys.exit(main())
