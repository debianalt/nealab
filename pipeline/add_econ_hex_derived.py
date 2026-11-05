"""
Desocupación y ocupados por hexágono en economic_activity (5-nov-2026).

Las pestañas Ocupados y Desocupación del ranking existían sólo por departamento
(build_empleo_rank.py, conteos por radio): con un departamento abierto el mapa no
tenía qué pintar. Las dos salen de columnas que cada celda ya tiene, todas del
mismo radio censal:

    tasa_desocupacion = 100 · (tasa_actividad − tasa_empleo) / tasa_actividad
                      = desocupados / activos del radio (las dos tasas son sobre 14+)
    ocupados_est      = tasa_empleo / 100 · pob_14_mas
                      = ocupados del radio repartidos por la población estimada de la celda

Donde el radio no publica tasa (menos de 30 en la base) las dos quedan vacías, como
las otras tasas (se rayan en el mapa).

FUENTE (--source-dir): una carpeta con lo ÚLTIMO PUBLICADO, con la estructura del CDN
(<t>/sat_economic_activity.parquet y <t>/sat_dpto/sat_economic_activity_<key>.parquet;
t = misiones, corrientes, chaco, formosa). El script sólo agrega dos columnas al final:
cada archivo derivado tiene que conservar filas, h3index y todas las columnas de la
fuente con los mismos valores, o frena.

TRAMPA que lo motivó (5-nov-2026): la primera versión bajaba lo servido con backup() de
refresh_dept_parquets.py, que nunca vuelve a bajar un respaldo existente. La carpeta
elegida (r2-econ-backup-2026-11) tenía copias del 2-nov y se publicó eso: durante unas
horas la capa perdió la grilla completa de Corrientes/Chaco/Formosa, el relleno censal
de Corrientes y las columnas de transiciones. Por eso: la fuente es explícita, y el
respaldo de lo que se reemplaza va a una carpeta NUEVA (si existe, frena).

    python pipeline/add_econ_hex_derived.py --source-dir pipeline/output/econ_source_v35 --dry-run
    python pipeline/add_econ_hex_derived.py --source-dir pipeline/output/econ_source_v35 --backup-dir C:/Users/ant/r2-econ-backup-2026-11-05-restore

Después: `npm run purge-cdn` y, si cambia el contenido de URLs ya servidas, subir el
?v de economic_activity y DEPT_V en src/lib/config.ts (+ deploy).
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, SRC_DATA_DIR, fetch

LAYER = 'economic_activity'
TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa']
NEW = ['tasa_desocupacion', 'ocupados_est']
# Columns the published layer must carry (transitions, 4-nov-2026): a source without them is an old copy.
REQUIRED = ['tasa_empleo', 'tasa_actividad', 'pob_14_mas', 'pct_universitario', 'viirs_mean_radiance',
            'pct_universitario_2010', 'viirs_2014_neto', 'viirs_2025_neto']
TERR_VS_DEPTS = 50      # territory rows vs Σ department rows (v35: Corrientes 1,035,346 vs 1,035,314)
# Controls against the summary (radio counts). Desocupación is exact per radio: any
# department off by more than TOL_DESOC_PTS stops the run. Ocupados depends on the cells'
# estimated population (radios under MIN_BASE carry no rate, cells whose dominant radio is
# in another department): departments beyond TOL_OCUP_PCT are reported, and only a
# territory total beyond TOL_OCUP_TERR stops (v35: −0.15 to −0.52 %).
TOL_OCUP_PCT = 2.0
TOL_OCUP_TERR = 1.0
TOL_DESOC_PTS = 1.0


def derive(df: pd.DataFrame) -> pd.DataFrame:
    act, emp, pob = df['tasa_actividad'], df['tasa_empleo'], df['pob_14_mas']
    ok = act.notna() & emp.notna() & (act > 0)
    out = df.drop(columns=[c for c in NEW if c in df.columns])
    out['tasa_desocupacion'] = np.where(ok, (100 * (act - emp) / act).clip(lower=0), np.nan).round(2)
    out['ocupados_est'] = np.where(emp.notna() & pob.notna(), emp / 100 * pob, np.nan).round(2)
    return out


def checked(src: pd.DataFrame, label: str) -> pd.DataFrame:
    """Derive and prove nothing but the two new columns changed."""
    missing = [c for c in REQUIRED if c not in src.columns]
    if missing:
        sys.exit(f'{label}: la fuente no tiene {missing} (¿copia vieja?)')
    out = derive(src)
    base = [c for c in src.columns if c not in NEW]
    if list(out.columns) != base + NEW:
        sys.exit(f'{label}: columnas {list(out.columns)} ≠ fuente + {NEW}')
    if len(out) != len(src) or not out['h3index'].equals(src['h3index']):
        sys.exit(f'{label}: filas o h3index distintos de la fuente')
    if not out[base].equals(src[base]):
        sys.exit(f'{label}: cambió algún valor de la fuente')
    return out


def main() -> None:
    # The report prints Σ and accents: a Windows console defaults to cp1252.
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser()
    ap.add_argument('--source-dir', required=True)
    ap.add_argument('--backup-dir', help='carpeta NUEVA para respaldar lo que hoy sirve R2 (obligatoria al subir)')
    ap.add_argument('--territory', choices=TERRITORIES, action='append')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    if not args.dry_run:
        if not args.backup_dir:
            sys.exit('--backup-dir es obligatorio al subir')
        if os.path.exists(args.backup_dir):
            sys.exit(f'{args.backup_dir} ya existe: usar una carpeta nueva (un respaldo viejo no es lo que hoy se sirve)')

    staged, problems, notes = [], [], []
    for t in args.territory or TERRITORIES:
        prefix = get_territory(t)['output_prefix']
        summary_path = os.path.join(SRC_DATA_DIR, f'sat_{LAYER}_dept_summary.json' if t == 'misiones'
                                    else f'{t}_sat_{LAYER}_summary.json')
        with open(summary_path, encoding='utf-8') as f:
            summary = json.load(f)
        src_dir = os.path.join(args.source_dir, t)
        stage = os.path.join(OUTPUT_DIR, 'econ_hex_derived', t)
        os.makedirs(os.path.join(stage, 'sat_dpto'), exist_ok=True)

        name = f'sat_{LAYER}.parquet'
        d = checked(pd.read_parquet(os.path.join(src_dir, name)), f'{t}/territorio')
        p = os.path.join(stage, name)
        d.to_parquet(p, index=False, compression='zstd')
        staged.append((p, f'data/{prefix}{name}'))
        print(f'\n{t}: territorio {len(d):,} celdas · desocupación con valor {d.tasa_desocupacion.notna().sum():,} '
              f'· P50/P90/P98 {np.nanpercentile(d.tasa_desocupacion, [50, 90, 98]).round(1).tolist()} %'
              f' · ocupados_est P50/P90/P98/máx {np.nanpercentile(d.ocupados_est, [50, 90, 98]).round(1).tolist()} / {d.ocupados_est.max():.0f}')

        terr_cells, terr_radios, dept_rows = 0.0, 0, 0
        for dep in summary['departments']:
            key = dep['parquetKey']
            dname = f'sat_{LAYER}_{key}.parquet'
            spath = os.path.join(src_dir, 'sat_dpto', dname)
            if not os.path.exists(spath):
                sys.exit(f'{t}/{key}: falta en la fuente ({spath})')
            new = checked(pd.read_parquet(spath), f'{t}/{key}')
            dept_rows += len(new)
            p = os.path.join(stage, 'sat_dpto', dname)
            new.to_parquet(p, index=False, compression='zstd')
            staged.append((p, f'data/{prefix}sat_dpto/{dname}'))

            r = dep.get('rank') or {}
            label = dep.get('dpto') or key
            ocup = new.ocupados_est.sum()
            if r.get('ocupados_n'):
                terr_cells += ocup
                terr_radios += r['ocupados_n']
                gap = 100 * (ocup - r['ocupados_n']) / r['ocupados_n']
                if abs(gap) > TOL_OCUP_PCT:
                    notes.append(f'{t}/{label}: Σ ocupados_est {ocup:,.0f} vs ocupados_n {r["ocupados_n"]:,} ({gap:+.1f} %)')
            m = new.tasa_desocupacion.notna() & new.pob_14_mas.notna()
            activos = (new.tasa_actividad / 100 * new.pob_14_mas)[m]
            if r.get('desocup_pct') is not None and activos.sum() > 0:
                des = float((new.tasa_desocupacion[m] / 100 * activos).sum() / activos.sum() * 100)
                if abs(des - r['desocup_pct']) > TOL_DESOC_PTS:
                    problems.append(f'{t}/{label}: desocupación ponderada {des:.1f} % vs desocup_pct {r["desocup_pct"]} %')
        if abs(len(d) - dept_rows) > TERR_VS_DEPTS:
            problems.append(f'{t}: territorio {len(d):,} filas vs Σ deptos {dept_rows:,}')
        if terr_radios:
            gap = 100 * (terr_cells - terr_radios) / terr_radios
            print(f'{t}: deptos {dept_rows:,} filas · Σ ocupados_est {terr_cells:,.0f} vs Σ ocupados_n {terr_radios:,} ({gap:+.2f} %)')
            if abs(gap) > TOL_OCUP_TERR:
                problems.append(f'{t}: Σ ocupados_est {gap:+.2f} % contra los radios')

    if notes:
        print('Departamentos cuyas celdas suman otra cantidad de ocupados que sus radios (aviso):\n  ' + '\n  '.join(notes))
    print(f'\n{len(staged)} archivos preparados en {os.path.join(OUTPUT_DIR, "econ_hex_derived")}')
    if problems:
        print('Controles fuera de tolerancia:\n  ' + '\n  '.join(problems))
        sys.exit(1)
    print('Controles OK: fuente intacta + 2 columnas; filas territorio = deptos; ocupados y desocupación contra el resumen.')
    if args.dry_run:
        return

    # Back up what R2 serves now, fresh, into the new folder; then publish.
    for p, key in staged:
        dest = os.path.join(args.backup_dir, key[len('data/'):])
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        fetch(f'{CDN}/{key[len("data/"):]}', dest)
    print(f'Respaldo de lo servido: {args.backup_dir} ({len(staged)} archivos)')
    from upload_to_r2 import upload_file
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{len(failed)} subidas fallaron: {failed[:5]}')
    print('Publicado. Falta: npm run purge-cdn (y ?v/DEPT_V + deploy si cambió el contenido de URLs servidas).')


if __name__ == '__main__':
    main()
