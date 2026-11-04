"""
Census cells missing from the served department parquets: fill them (4-nov-2026).

Found while building the census radio unit (build_census_radios.py --check): in Corrientes,
economic_activity serves 49.425 cells of the dasymetric crosswalk with an empty rate although the
radio has one (rows of the full grid, columns never filled), and sociodemographic lacks 74.488
crosswalk cells as rows (28.358 of them in radios with a rate): together ~1,4 % of the province's
census base, 10,5 % in Esquina, because refresh_dept_parquets.py freezes department membership to
what the CDN serves.

Per layer and territory, starting from the files the CDN serves (no other row or column changes):
  - every crosswalk cell gets the rates build_census_rates.py computes (Σw·num / Σw·den, same
    definitions and MIN_BASE), its base columns and derived columns (density, household size);
  - an existing row only changes where its rate was empty and is now computable;
  - a missing cell becomes a new row in the department of its dominant radio (most buildings),
    with the other columns empty except n_edificios / hogares_estimados when the layer has them;
  - the typology (type / type_label, k-means) of a newly valued cell is the most frequent one among
    the already typed cells of its radio, else of its queen-neighbour radios, else of its department
    (decided with the author, 4-nov-2026: the rates are the radio's, so the type nearly was).

Usage:
    python pipeline/fill_census_rows.py --territory corrientes --layer economic_activity --served-dir DIR
    (DIR holds the served copies: sat_<layer>.parquet and sat_dpto/sat_<layer>_<key>.parquet, as
     backed up from cdn.spatia.ar). Writes pipeline/output/fill_census_rows/<t>/… and prints checks.
    --upload publishes those files to R2 (data/<prefix>…) after the checks pass.
"""
import argparse
import glob
import os
import sys

import h3
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from build_census_rates import DERIVED, MIN_BASE, RATES, TERRITORIES, crosswalk_path, hex_sums, radio_counts  # noqa: E402
from config import OUTPUT_DIR, get_territory  # noqa: E402

BASE_OF = {'HOG': 'hog_total'}


def radios_unit(t: str) -> pd.DataFrame:
    prefix = get_territory(t)['output_prefix']
    p = os.path.join(OUTPUT_DIR, prefix, 'census_radios_2022.parquet') if prefix else os.path.join(OUTPUT_DIR, 'census_radios_2022.parquet')
    return pd.read_parquet(p, columns=['redcode', 'dpto_key', 'vecinos']).set_index('redcode')


def computed(layer: str, t: str, counts: pd.DataFrame) -> pd.DataFrame:
    """Every crosswalk cell's census columns for the layer, as build_census_rates.py computes them."""
    codprov = TERRITORIES[t]
    radios = counts[counts.redcode.str.startswith(codprov)]
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight'])
    xw['redcode'] = xw.redcode.astype(str)
    m = xw.merge(radios, on='redcode', how='inner')
    out = pd.DataFrame(index=pd.Index(sorted(xw.h3index.unique()), name='h3index'))
    for col, (num, (den, base), min_base) in RATES[layer].items():
        g = hex_sums(m, num, den, min_base)
        out[col] = (g.n / g.d * 100).round(1)
        if base not in out:
            out[base] = g.d.round(1)
    for kind, col in DERIVED.get(layer, []):
        if kind == 'density':
            g = hex_sums(m, None, 'p_total', 0)
            area = pd.Series({h: h3.cell_area(h, 'km^2') for h in out.index})
            out[col] = (g.d / area).round(1)
        elif kind == 'hh_size':
            g = hex_sums(m, 'p_total', 'h_total', MIN_BASE)
            out[col] = (g.n / g.d).round(2)
    return out


def dominant_radio(t: str) -> pd.Series:
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight', 'n_buildings'])
    xw['redcode'] = xw.redcode.astype(str)
    return xw.sort_values(['h3index', 'n_buildings', 'weight'], ascending=[True, False, False]) \
             .drop_duplicates('h3index').set_index('h3index').redcode


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', required=True, choices=list(TERRITORIES))
    ap.add_argument('--layer', required=True, choices=list(RATES))
    ap.add_argument('--served-dir', required=True)
    ap.add_argument('--upload', action='store_true')
    a = ap.parse_args()
    t, L, S = a.territory, a.layer, a.served_dir
    prefix = get_territory(t)['output_prefix']
    out_dir = os.path.join(OUTPUT_DIR, 'fill_census_rows', t)
    os.makedirs(os.path.join(out_dir, 'sat_dpto'), exist_ok=True)

    terr = pd.read_parquet(os.path.join(S, f'sat_{L}.parquet'))
    files = sorted(glob.glob(os.path.join(S, 'sat_dpto', f'sat_{L}_*.parquet')))
    depts = {os.path.basename(f)[len(f'sat_{L}_'):-len('.parquet')]: pd.read_parquet(f) for f in files}
    cols = list(terr.columns)
    rate_cols = [c for c in RATES[L]] + [c for _, c in DERIVED.get(L, []) if c in cols]
    rate_cols = [c for c in rate_cols if c in cols]
    base_cols = [c for c in cols if c in {b for _, (_, (_, b), _) in RATES[L].items()}]
    calc = computed(L, t, radio_counts())
    calc = calc[[c for c in rate_cols + base_cols if c in calc]]
    # 'c_teen_pregnancy'-like columns not served are ignored; served rate columns not computed stay as served.
    shown = [c for c in rate_cols if c in calc and c not in {'densidad_hab_km2'}]
    valued = calc[shown].notna().any(axis=1)
    dom = dominant_radio(t)
    ru = radios_unit(t)

    served_idx = set(terr.h3index)
    dept_of = {h: k for k, d in depts.items() for h in d.h3index}
    has_type = 'type' in cols and 'type_label' in cols

    # Rows to touch: served rows whose shown rates are all empty but computable, and missing valued cells.
    terr_i = terr.set_index('h3index')
    empty = terr_i[shown].isna().all(axis=1)
    fill_existing = [h for h in terr_i.index[empty] if h in calc.index and valued.get(h, False)]
    add_new = [h for h in calc.index[valued] if h not in served_idx]
    print(f'{t}/{L}: servidas {len(terr):,} filas; a completar {len(fill_existing):,}; a agregar {len(add_new):,}')

    # Typology of newly valued cells: mode among typed cells of the radio → queen neighbours → department.
    touched = fill_existing + add_new
    if has_type:
        typed = terr_i[terr_i['type'].notna() & terr_i['type_label'].notna()]
        typed = typed.assign(radio=typed.index.map(dom)).dropna(subset=['radio'])
        pair = typed[['type', 'type_label']].astype(str).agg('|'.join, axis=1)
        mode_radio = pair.groupby(typed.radio).agg(lambda s: s.value_counts().index[0])
        typed_k = pd.Series({h: dept_of.get(h) for h in typed.index})
        mode_dept = pair.groupby(typed_k).agg(lambda s: s.value_counts().index[0])

        def type_for(h):
            r = dom.get(h)
            if r in mode_radio.index:
                return mode_radio[r], 'radio'
            if r in ru.index:
                nb = [n for n in ru.at[r, 'vecinos'] if n in mode_radio.index]
                if nb:
                    return pd.Series([mode_radio[n] for n in nb]).value_counts().index[0], 'vecinos'
            k = dept_of.get(h) or (ru.at[r, 'dpto_key'] if r in ru.index else None)
            return (mode_dept[k], 'depto') if k in mode_dept.index else (None, 'sin tipo')
        types = {h: type_for(h) for h in touched}
        src = pd.Series([v[1] for v in types.values()]).value_counts().to_dict()
        print(f'  tipología de las celdas tocadas: {src}')
    tdtype, ldtype = terr['type'].dtype if has_type else None, None

    # Build the new territory frame.
    new_terr = terr_i.copy()
    for h in fill_existing:
        for c in calc.columns:
            if pd.isna(new_terr.at[h, c]) and pd.notna(calc.at[h, c]):
                new_terr.at[h, c] = calc.at[h, c]
    add = pd.DataFrame(index=pd.Index(add_new, name='h3index'), columns=new_terr.columns)
    for c in calc.columns:
        add[c] = calc.loc[add_new, c].values
    if 'n_edificios' in add or 'hogares_estimados' in add:
        xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'n_buildings', 'est_hogares'])
        s = xw.groupby('h3index')[['n_buildings', 'est_hogares']].sum()
        if 'n_edificios' in add: add['n_edificios'] = s.n_buildings.reindex(add_new).round().fillna(0).astype('int64').values
        if 'hogares_estimados' in add: add['hogares_estimados'] = s.est_hogares.reindex(add_new).round().fillna(0).astype('int64').values
    new_terr = pd.concat([new_terr, add])
    if has_type:
        for h, (v, _) in types.items():
            if v is None:
                continue
            ty, lab = v.split('|', 1)
            if pd.isna(new_terr.at[h, 'type']) or h in add_new:
                new_terr.at[h, 'type'] = float(ty)
                new_terr.at[h, 'type_label'] = lab
    new_terr = new_terr.reset_index()
    for c in cols:
        if c == 'h3index':
            continue
        try:
            new_terr[c] = new_terr[c].astype(terr[c].dtype)
        except (TypeError, ValueError) as e:
            sys.exit(f'{c}: no se pudo mantener el tipo {terr[c].dtype}: {e}')
    new_terr = new_terr[cols]

    # Department files: served rows (updated in place) + new rows in their dominant radio's department.
    new_dept_of = dict(dept_of)
    sin_k = 0
    for h in add_new:
        r = dom.get(h)
        k = ru.at[r, 'dpto_key'] if r in ru.index else None
        if k in depts:
            new_dept_of[h] = k
        else:
            sin_k += 1
    nt = new_terr.set_index('h3index')
    for k, d in depts.items():
        hs = list(d.h3index) + [h for h in add_new if new_dept_of.get(h) == k]
        out = nt.loc[hs].reset_index()[cols]
        out.to_parquet(os.path.join(out_dir, 'sat_dpto', f'sat_{L}_{k}.parquet'), index=False)
    new_terr.to_parquet(os.path.join(out_dir, f'sat_{L}.parquet'), index=False)

    # Checks: untouched rows identical; census base now matches the radios; departments.
    old = terr_i
    keep = [h for h in old.index if h not in set(fill_existing)]
    same = nt.loc[keep, [c for c in cols if c != 'h3index']].equals(old.loc[keep, [c for c in cols if c != 'h3index']])
    counts = radio_counts()
    rc = counts[counts.redcode.str.startswith(TERRITORIES[t])]
    pv, (num, (den, base), mb) = next(iter(RATES[L].items()))
    def share(df):
        v = df[df[pv].notna()]
        return v[base].sum() / rc.loc[rc[den] >= mb, den].sum() * 100
    print(f'  filas no tocadas idénticas: {same}; sin depto: {sin_k}')
    print(f'  base censal en celdas con {pv}: antes {share(terr):.2f} %, después {share(new_terr):.2f} %')
    if not same or sin_k:
        sys.exit('chequeo fallido')
    print(f'  → {out_dir}')

    if a.upload:
        from upload_to_r2 import upload_file
        ok = upload_file(os.path.join(out_dir, f'sat_{L}.parquet'), f'data/{prefix}sat_{L}.parquet')
        for k in depts:
            ok &= upload_file(os.path.join(out_dir, 'sat_dpto', f'sat_{L}_{k}.parquet'), f'data/{prefix}sat_dpto/sat_{L}_{k}.parquet')
        if not ok:
            sys.exit('subida incompleta')
        print('  subido; falta npm run purge-cdn, subir DEPT_V y ?v, build_layer_ranks, ficha_link_index, Hexoma')


if __name__ == '__main__':
    main()
