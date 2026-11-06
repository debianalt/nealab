"""
Corrientes, tramo B (6-nov-2026): the census layers on the cells of the new crosswalk.

The 6-nov reload of GBA in Corrientes (corrected bbox) rebuilt the dasymetric crosswalk
(133,856 → 52,227 cells: the south and east had no buildings, so their radios were spread
over every cell by area). The census layers still served the old grid: each rural radio
painted all its cells, and the radio selection (cells of the new crosswalk) did not match
what the layer painted. build_census_rates.py and refresh_dept_parquets.py keep the served
rows and fill_census_rows.py only adds them, so taking cells out needs this script.

Target, as in Chaco and Formosa:
  service_deprivation, health_access, education_capital, education_flow, sociodemographic
      rows = the crosswalk cells, exactly;
  economic_activity
      the full night-lights grid (served rows + crosswalk cells it lacked), census columns
      only on crosswalk cells, n_edificios / hogares_estimados = Σ of the crosswalk (0 off it);
  censo_temporal
      build_censo_temporal.build_territory on the new crosswalk universe.

Per cell:
  - rates, bases, density, household size: fill_census_rows.computed() (the definitions and
    MIN_BASE of build_census_rates.py) on the new crosswalk, every cell (the weights changed);
  - 2010 columns: build_census_trayectorias.rates_2010() on the new crosswalk;
  - tasa_desocupacion, ocupados_est: add_econ_hex_derived.derive();
  - VIIRS and c_healthcare_time: served value on the cells that stay; on new cells VIIRS is
    sampled from the same rasters (only if that reproduces served cells) and the health time is
    the Oxford minutes of the cell's radios weighted by buildings;
  - type / type_label: served on crosswalk cells, empty off it; a new cell takes the mode of its
    radio's typed cells, else of its queen neighbours, else of its department (4-nov-2026 rule).
    Codes are never averaged.
Departments: one cell → department map for the six layers. Cells that stay keep their served
department; new cells go to their dominant radio's dpto_key (census_radios_2022.parquet).

Source and backup (the 5-nov-2026 regression came from deriving from a stale backup): the
served files are downloaded twice in this run, from cdn.spatia.ar (with a buster) into
<backup-dir>/backup and from the R2 origin into <backup-dir>/source; both copies must be
identical, and everything derives from source/. <backup-dir> must not exist. Nothing reads or
writes pipeline/output/corrientes/sat_*.parquet.

    python pipeline/regrid_census_corrientes.py download --backup-dir C:/Users/ant/r2-census-regrid-backup-2026-11-06-HHMM
    python pipeline/regrid_census_corrientes.py build    --backup-dir <same>
    python pipeline/regrid_census_corrientes.py upload   --backup-dir <same>     (after the checks)
Staging: pipeline/output/regrid_corrientes_20261106/<R2 key without data/>.
"""
import argparse
import json
import os
import shutil
import sys
import time
import urllib.request
from urllib.parse import quote

import h3
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from build_census_rates import DERIVED, MIN_BASE, RATES, TERRITORIES, crosswalk_path, radio_counts  # noqa: E402
from config import OUTPUT_DIR  # noqa: E402
from fill_census_rows import computed, dominant_radio, radios_unit  # noqa: E402
from refresh_dept_parquets import CDN, SRC_DATA_DIR  # noqa: E402
from upload_to_r2 import R2_PUBLIC_BASE  # noqa: E402

T = 'corrientes'
PREFIX = 'corrientes/'
CODPROV = TERRITORIES[T]
LAYERS = ['economic_activity', 'service_deprivation', 'health_access', 'education_capital',
          'education_flow', 'sociodemographic']
TEMPORAL = 'censo_temporal'
STAGE = os.path.join(OUTPUT_DIR, 'regrid_corrientes_20261106')
TRAY = {   # build_census_trayectorias.LAYERS[...]['rates'], imported lazily (geopandas, PostGIS)
    'economic_activity': 'economic_activity',
    'education_capital': 'education_capital',
}


def summary_path(layer: str) -> str:
    return os.path.join(SRC_DATA_DIR, f'{T}_sat_{layer}_summary.json')


def load_summary(layer: str) -> dict:
    with open(summary_path(layer), encoding='utf-8') as f:
        return json.load(f)


def keys(layer: str) -> list[str]:
    """R2 keys (without data/) the layer serves in Corrientes."""
    out = [f'{PREFIX}sat_{layer}.parquet']
    if layer != TEMPORAL:
        out += [f'{PREFIX}sat_dpto/sat_{layer}_{d["parquetKey"]}.parquet' for d in load_summary(layer)['departments']]
    return out


def get(url: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    req = urllib.request.Request(url, headers={'User-Agent': 'spatia-pipeline/1.0'})
    with urllib.request.urlopen(req, timeout=300) as r, open(path, 'wb') as f:
        f.write(r.read())


def download(bdir: str) -> None:
    if os.path.exists(bdir):
        sys.exit(f'{bdir} ya existe: usar una carpeta nueva (un respaldo viejo no es lo que hoy se sirve)')
    n = 0
    for layer in LAYERS + [TEMPORAL]:
        for k in keys(layer):
            buster = f'fresh={int(time.time() * 1000)}'
            b, s = os.path.join(bdir, 'backup', k), os.path.join(bdir, 'source', k)
            get(f'{CDN}/{quote(k)}?{buster}', b)
            get(f'{R2_PUBLIC_BASE}/data/{quote(k)}?{buster}', s)
            with open(b, 'rb') as fb, open(s, 'rb') as fs:
                if fb.read() != fs.read():
                    sys.exit(f'{k}: la copia del CDN y la del origen R2 difieren')
            n += 1
    print(f'{n} archivos servidos bajados dos veces (CDN y origen R2), idénticos: {bdir}')


# ── build ─────────────────────────────────────────────────────────────────────

def read_source(bdir: str, layer: str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    src = os.path.join(bdir, 'source', PREFIX)
    terr = pd.read_parquet(os.path.join(src, f'sat_{layer}.parquet'))
    depts = {}
    if layer != TEMPORAL:
        for d in load_summary(layer)['departments']:
            k = d['parquetKey']
            depts[k] = pd.read_parquet(os.path.join(src, 'sat_dpto', f'sat_{layer}_{k}.parquet'))
    return terr, depts


def dept_map(bdir: str, xcells: set) -> tuple[dict, dict]:
    """One cell → department map for the crosswalk cells (served department, checked across the
    six layers; new cells → dominant radio's dpto_key) and economic_activity's own map off it."""
    served: dict[str, dict] = {}
    for layer in LAYERS:
        _, depts = read_source(bdir, layer)
        m = {}
        for k, d in depts.items():
            for h in d.h3index:
                if h in m and m[h] != k:
                    sys.exit(f'{layer}: {h} en dos departamentos ({m[h]}, {k})')
                m[h] = k
        served[layer] = m
    ref = served['service_deprivation']
    for layer, m in served.items():
        diff = [h for h in xcells if h in m and h in ref and m[h] != ref[h]]
        if diff:
            print(f'  aviso {layer}: {len(diff)} celdas del crosswalk en otro depto que service_deprivation (se unifica)')
    merged = {}
    for layer in ['service_deprivation', 'health_access', 'education_capital', 'education_flow',
                  'sociodemographic', 'economic_activity']:
        for h, k in served[layer].items():
            if h in xcells and h not in merged:
                merged[h] = k
    dom = dominant_radio(T)
    ru = radios_unit(T)
    valid = {d['parquetKey'] for d in load_summary('service_deprivation')['departments']}
    new = [h for h in xcells if h not in merged]
    for h in new:
        k = ru.at[dom[h], 'dpto_key'] if dom.get(h) in ru.index else None
        if k not in valid:
            sys.exit(f'{h}: sin depto (radio {dom.get(h)}, clave {k})')
        merged[h] = k
    print(f'  departamentos: {len(merged) - len(new):,} celdas con el depto servido, {len(new):,} nuevas por radio dominante')
    return merged, served['economic_activity']


def type_modes(served_x: pd.DataFrame, dmap: dict) -> callable:
    """fill_census_rows.py's typology rule: radio → queen neighbours → department."""
    dom = dominant_radio(T)
    ru = radios_unit(T)
    typed = served_x[served_x['type'].notna() & served_x['type_label'].notna()]
    pair = typed['type'].astype(float).astype(str) + '|' + typed['type_label'].astype(str)
    radio = typed.h3index.map(dom)
    mode_radio = pair.groupby(radio.values).agg(lambda s: s.value_counts().index[0])
    mode_dept = pair.groupby(typed.h3index.map(dmap).values).agg(lambda s: s.value_counts().index[0])

    def f(h):
        r = dom.get(h)
        if r in mode_radio.index:
            return mode_radio[r], 'radio'
        if r in ru.index:
            nb = [n for n in ru.at[r, 'vecinos'] if n in mode_radio.index]
            if nb:
                return pd.Series([mode_radio[n] for n in nb]).value_counts().index[0], 'vecinos'
        k = dmap.get(h)
        return (mode_dept[k], 'depto') if k in mode_dept.index else (None, 'sin tipo')
    return f


def oxford_new(cells: list) -> pd.Series:
    from build_census_radios import oxford_minutes
    om = oxford_minutes(T)
    xw = pd.read_parquet(crosswalk_path(T), columns=['h3index', 'redcode', 'n_buildings'])
    xw = xw[xw.h3index.isin(set(cells))].copy()
    xw['redcode'] = xw.redcode.astype(str)
    xw['m'] = xw.redcode.map(om)
    xw = xw[xw.m.notna()]
    xw['b'] = xw.n_buildings.astype(float).where(xw.n_buildings > 0, 1e-9)
    g = xw.groupby('h3index')
    return ((xw.m * xw.b).groupby(xw.h3index).sum() / g.b.sum()).round(1)


def viirs_new(served: pd.DataFrame, cells: list) -> pd.DataFrame | None:
    """VIIRS of new cells, as compute_viirs_buildings_h3.py / build_census_trayectorias.add_viirs
    sampled them, only if the same sampling gives back the served cells."""
    from compute_viirs_buildings_h3 import sample_raster_bilinear
    tdir = os.path.join(OUTPUT_DIR, T)
    tifs = {'viirs_mean_radiance': os.path.join(tdir, f'viirs_{T}_2025.tif'),
            'viirs_2014': os.path.join(tdir, f'viirs_{T}_2014.tif')}
    probe = served[served.viirs_mean_radiance.notna()].sample(5000, random_state=1)
    out = pd.DataFrame(index=pd.Index(cells, name='h3index'))
    for col, tif in tifs.items():
        if not os.path.exists(tif):
            print(f'  VIIRS: falta {tif}; las celdas nuevas quedan sin VIIRS')
            return None
        ll = np.array([h3.cell_to_latlng(h) for h in probe.h3index])
        v = np.round(np.nan_to_num(sample_raster_bilinear(tif, ll[:, 1], ll[:, 0]), nan=0.0), 4)
        ok = np.isclose(v, probe[col].values, atol=1e-4).mean()
        print(f'  VIIRS {col}: el muestreo reproduce {100 * ok:.2f} % de 5.000 celdas servidas')
        if ok < 0.999:
            return None
        ll = np.array([h3.cell_to_latlng(h) for h in cells])
        out[col] = np.round(np.nan_to_num(sample_raster_bilinear(tif, ll[:, 1], ll[:, 0]), nan=0.0), 4)
    for col, net in (('viirs_2014', 'viirs_2014_neto'), ('viirs_mean_radiance', 'viirs_2025_neto')):
        s = served[served[net] > 0]
        fondo = (s[col] - s[net]).round(4)
        if fondo.max() - fondo.min() > 2e-4:
            print(f'  VIIRS {net}: el fondo servido no es constante ({fondo.min()}–{fondo.max()}); celdas nuevas sin VIIRS')
            return None
        out[net] = (out[col] - fondo.median()).clip(lower=0).round(4)
    return out


def cast_like(df: pd.DataFrame, like: pd.DataFrame, label: str) -> pd.DataFrame:
    df = df[list(like.columns)]
    for c in like.columns:
        if df[c].dtype != like[c].dtype:
            try:
                df[c] = df[c].astype(like[c].dtype)
            except (TypeError, ValueError) as e:
                sys.exit(f'{label}/{c}: no se pudo mantener el tipo {like[c].dtype}: {e}')
    return df


def build_layer(bdir: str, layer: str, xw: pd.DataFrame, counts: pd.DataFrame, dmap: dict, econ_dmap: dict) -> dict:
    terr, depts = read_source(bdir, layer)
    cols = list(terr.columns)
    X = set(xw.h3index)
    xs = xw.groupby('h3index')[['n_buildings', 'est_hogares']].sum()
    econ = layer == 'economic_activity'
    s = terr.set_index('h3index')
    if econ:
        new_cells = sorted(X - set(s.index))
        rows = list(terr.h3index) + new_cells
    else:
        kept = [h for h in terr.h3index if h in X]
        new_cells = sorted(X - set(kept))
        rows = kept + new_cells
    df = s.reindex(rows)                      # served values on the cells that stay, empty on new ones
    df.index.name = 'h3index'
    handled = set()

    # Census columns: recomputed on the new crosswalk, empty off it.
    calc = computed(layer, T, counts)
    census = [c for c in calc.columns if c in cols]
    df[census] = calc[census].reindex(df.index).values
    handled |= set(census)
    for c in ('n_edificios', 'hogares_estimados'):
        if c in cols:
            src = 'n_buildings' if c == 'n_edificios' else 'est_hogares'
            df[c] = xs[src].reindex(df.index).fillna(0).round().values
            handled.add(c)
    if 'building_density_per_km2' in cols:
        area = np.array([h3.cell_area(h, 'km^2') for h in df.index])
        df['building_density_per_km2'] = np.round(df['n_edificios'].values / area, 2)
        handled.add('building_density_per_km2')

    # 2010 columns.
    if layer in TRAY:
        from build_census_trayectorias import LAYERS as TL, rates_2010
        for c, v in rates_2010(T, CODPROV, pd.Series(df.index, index=df.index), TL[TRAY[layer]]['rates']).items():
            df[c] = v.values
            handled.add(c)

    # Typology.
    src_x = terr[terr.h3index.isin(X)]
    if 'type' in cols:
        off = ~df.index.isin(list(X))
        df.loc[off, ['type', 'type_label']] = np.nan
        f = type_modes(src_x, dmap)
        tally = {}
        for h in new_cells:
            v, how = f(h)
            tally[how] = tally.get(how, 0) + 1
            if v is not None:
                ty, lab = v.split('|', 1)
                df.at[h, 'type'] = float(ty)
                df.at[h, 'type_label'] = lab
        print(f'  {layer}: tipología de {len(new_cells):,} celdas nuevas: {tally}')
        handled |= {'type', 'type_label'}

    # Non-census columns on new cells.
    if 'c_healthcare_time' in cols:
        df.loc[new_cells, 'c_healthcare_time'] = oxford_new(new_cells).reindex(new_cells).values
        handled.add('c_healthcare_time')
    if econ:
        vcols = ['viirs_mean_radiance', 'viirs_2014', 'viirs_2014_neto', 'viirs_2025_neto']
        v = viirs_new(terr, new_cells) if new_cells else None
        if v is not None:
            for c in vcols:
                df.loc[new_cells, c] = v[c].values
        handled |= set(vcols)
        from add_econ_hex_derived import derive
        df = derive(df.reset_index()).set_index('h3index')
        handled |= {'tasa_desocupacion', 'ocupados_est'}

    rest = [c for c in cols if c != 'h3index' and c not in handled]
    if rest:
        print(f'  {layer}: columnas servidas sin regla propia (valor servido, vacías en celdas nuevas): {rest}')
    out = cast_like(df.reset_index(), terr, layer)

    # Departments.
    o = out.set_index('h3index')
    # New department of every row: crosswalk cells → the common map; economic_activity's cells
    # off the crosswalk → the department that served them (none if no department file had them).
    new_k = pd.Series({h: dmap[h] for h in X}) if not econ else \
        pd.Series(out.h3index.map(lambda h: dmap[h] if h in X else econ_dmap.get(h)).values, index=out.h3index.values)
    new_k = new_k.dropna()
    files, hex_count = {}, {}
    for k, d in depts.items():
        stay = d.h3index[d.h3index.map(new_k).eq(k).values].tolist()
        sset = set(stay)
        mine = new_k.index[new_k.values == k]
        hs = stay + sorted(h for h in mine if h not in sset)
        files[k] = cast_like(o.loc[hs].reset_index(), d, f'{layer}/{k}')
        hex_count[k] = len(hs)
    in_depts = sum(len(v) for v in files.values())
    print(f'  {layer}: {len(terr):,} → {len(out):,} filas (Σ deptos {in_depts:,}); celdas nuevas {len(new_cells):,}')

    # Write staging.
    base = os.path.join(STAGE, PREFIX)
    os.makedirs(os.path.join(base, 'sat_dpto'), exist_ok=True)
    out.to_parquet(os.path.join(base, f'sat_{layer}.parquet'), index=False, compression='zstd')
    for k, d in files.items():
        d.to_parquet(os.path.join(base, 'sat_dpto', f'sat_{layer}_{k}.parquet'), index=False, compression='zstd')
    summ = load_summary(layer)
    for d in summ['departments']:
        d['hex_count'] = hex_count[d['parquetKey']]
    ranks_from_stage(layer, summ, base)
    with open(os.path.join(STAGE, f'{T}_sat_{layer}_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summ, f, ensure_ascii=False, indent=2)
    return {'terr': terr, 'out': out, 'new_cells': new_cells}


def ranks_from_stage(layer: str, summ: dict, base: str) -> None:
    """The layer's rank fields of build_layer_ranks.py (Σ rate·base / Σ base of the department's
    cells), from the staged department files. On the old grid they were off the radio counts by up
    to 1.3 pts (sin cloacas, Sauce); on the new one by ≤ 0.05 (≤ 0.21 teen motherhood, MIN_BASE 1)."""
    import duckdb
    from build_layer_ranks import SPECS, clean
    spec = SPECS[layer]
    sel = ', '.join(f'{e} AS {f}' for f, (e, _) in spec.items())
    con = duckdb.connect()
    for d in summ['departments']:
        p = os.path.join(base, 'sat_dpto', f'sat_{layer}_{d["parquetKey"]}.parquet').replace('\\', '/')
        r = con.execute(f"SELECT {sel} FROM read_parquet('{p}')").df().iloc[0]
        vals = {}
        for f, (_, dec) in spec.items():
            v = r[f]
            vals[f] = None if v is None or v != v else (round(float(v), dec) if dec else int(round(float(v))))
        d['rank'] = {**(d.get('rank') or {}), **vals}
    dropped = clean(layer, summ, T)
    if dropped:
        print(f'  {layer}: rank sin dato para {dropped}')


def build_temporal(bdir: str) -> None:
    import duckdb
    from build_censo_temporal import build_territory
    terr, _ = read_source(bdir, TEMPORAL)
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    df = cast_like(build_territory(con, T), terr, TEMPORAL)
    base = os.path.join(STAGE, PREFIX)
    os.makedirs(base, exist_ok=True)
    df.to_parquet(os.path.join(base, f'sat_{TEMPORAL}.parquet'), index=False)
    print(f'  {TEMPORAL}: {len(terr):,} → {len(df):,} filas')


def build(bdir: str, only: list | None) -> None:
    if not os.path.isdir(os.path.join(bdir, 'source')):
        sys.exit(f'falta {bdir}/source: correr primero «download»')
    sys.stdout.reconfigure(encoding='utf-8')
    xw = pd.read_parquet(crosswalk_path(T))
    X = set(xw.h3index)
    print(f'crosswalk nuevo: {len(X):,} celdas')
    dmap, econ_dmap = dept_map(bdir, X)
    counts = radio_counts()
    for layer in only or LAYERS:
        if layer == TEMPORAL:
            continue
        build_layer(bdir, layer, xw, counts, dmap, econ_dmap)
    if not only or TEMPORAL in only:
        build_temporal(bdir)


# ── upload ────────────────────────────────────────────────────────────────────

def upload(bdir: str) -> None:
    from upload_to_r2 import upload_file
    staged = []
    for layer in LAYERS + [TEMPORAL]:
        for k in keys(layer):
            p = os.path.join(STAGE, k)
            if not os.path.exists(p):
                sys.exit(f'falta {p}')
            if not os.path.exists(os.path.join(bdir, 'backup', k)):
                sys.exit(f'{k}: no hay respaldo en {bdir}/backup')
            staged.append((p, f'data/{k}'))
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{len(failed)} subidas fallaron: {failed[:5]}')
    for layer in LAYERS:
        shutil.copyfile(os.path.join(STAGE, f'{T}_sat_{layer}_summary.json'), summary_path(layer))
    with open(os.path.join(STAGE, 'uploaded_keys.txt'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(k for _, k in staged) + '\n')
    print(f'{len(staged)} archivos subidos; resúmenes copiados a src/lib/data. Falta purgar el CDN '
          f'(claves en {STAGE}/uploaded_keys.txt), ?v y DEPT_V.')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('step', choices=['download', 'build', 'upload'])
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--layer', action='append', choices=LAYERS + [TEMPORAL])
    a = ap.parse_args()
    if a.step == 'download':
        download(a.backup_dir)
    elif a.step == 'build':
        build(a.backup_dir, a.layer)
    else:
        upload(a.backup_dir)


if __name__ == '__main__':
    main()
