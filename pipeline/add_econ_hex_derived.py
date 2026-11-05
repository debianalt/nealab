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

Trabaja sobre los parquets que sirve el CDN (territorio + cada departamento), no
sobre pipeline/output: sólo agrega dos columnas al final, sin re-partir
departamentos ni tocar las demás. Respalda lo servido, controla cada departamento
contra el resumen (ocupados_n, desocup_pct) y sólo después sube.

    python pipeline/add_econ_hex_derived.py --backup-dir C:/Users/ant/r2-econ-backup-2026-11 --dry-run
    python pipeline/add_econ_hex_derived.py --backup-dir C:/Users/ant/r2-econ-backup-2026-11

Después: subir DEPT_V y el ?v de economic_activity en src/lib/config.ts y
`npm run purge-cdn` (el borde cachea un día).
"""
import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, SRC_DATA_DIR, backup

LAYER = 'economic_activity'
TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa']
NEW = ['tasa_desocupacion', 'ocupados_est']
# Controls against the summary (radio counts). Desocupación is exact per radio: any
# department off by more than TOL_DESOC_PTS stops the run. Ocupados depends on the
# cells' estimated population (pob_14_mas): 5-nov-2026, Corrientes' cells carry 1.8 %
# fewer people 14+ than its radios, up to −16 % in Itatí, Sauce, Monte Caseros and
# Esquina (radios with few or no cells). That gap is in the served layer, not in this
# derivation, and rescaling would put people where there are no buildings: departments
# beyond TOL_OCUP_PCT are reported, and only a territory total beyond TOL_OCUP_TERR stops.
TOL_OCUP_PCT = 2.0      # % of ocupados_n, per department (reported)
TOL_OCUP_TERR = 2.5     # % of Σ ocupados_n, per territory (stops)
TOL_DESOC_PTS = 1.0     # points of desocup_pct


def derive(df: pd.DataFrame) -> pd.DataFrame:
    act, emp, pob = df['tasa_actividad'], df['tasa_empleo'], df['pob_14_mas']
    ok = act.notna() & emp.notna() & (act > 0)
    out = df.drop(columns=[c for c in NEW if c in df.columns])
    out['tasa_desocupacion'] = np.where(ok, (100 * (act - emp) / act).clip(lower=0), np.nan).round(2)
    out['ocupados_est'] = np.where(emp.notna() & pob.notna(), emp / 100 * pob, np.nan).round(2)
    return out


def main() -> None:
    # The report prints Σ and accents: a Windows console defaults to cp1252.
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser()
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--territory', choices=TERRITORIES, action='append')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    staged, problems, notes = [], [], []
    for t in args.territory or TERRITORIES:
        prefix = get_territory(t)['output_prefix']
        summary_path = os.path.join(SRC_DATA_DIR, f'sat_{LAYER}_dept_summary.json' if t == 'misiones'
                                    else f'{t}_sat_{LAYER}_summary.json')
        with open(summary_path, encoding='utf-8') as f:
            summary = json.load(f)
        bdir = os.path.join(args.backup_dir, t)
        stage = os.path.join(OUTPUT_DIR, 'econ_hex_derived', t)
        os.makedirs(os.path.join(bdir, 'sat_dpto'), exist_ok=True)
        os.makedirs(os.path.join(stage, 'sat_dpto'), exist_ok=True)

        name = f'sat_{LAYER}.parquet'
        backup(f'{CDN}/{prefix}{name}', os.path.join(bdir, name))
        main_df = derive(pd.read_parquet(os.path.join(bdir, name)))
        p = os.path.join(stage, name)
        main_df.to_parquet(p, index=False)
        staged.append((p, f'data/{prefix}{name}'))
        d = main_df
        print(f'\n{t}: territorio {len(d):,} celdas · desocupación con valor {d.tasa_desocupacion.notna().sum():,} '
              f'· P50/P90/P98 {np.nanpercentile(d.tasa_desocupacion, [50, 90, 98]).round(1).tolist()} %'
              f' · ocupados_est P50/P90/P98/máx {np.nanpercentile(d.ocupados_est, [50, 90, 98]).round(1).tolist()} / {d.ocupados_est.max():.0f}'
              f' · Σ {d.ocupados_est.sum():,.0f}')

        terr_cells, terr_radios = 0.0, 0
        for dep in summary['departments']:
            key = dep['parquetKey']
            dname = f'sat_{LAYER}_{key}.parquet'
            bpath = os.path.join(bdir, 'sat_dpto', dname)
            backup(f'{CDN}/{prefix}sat_dpto/{dname}', bpath)
            old = pd.read_parquet(bpath)
            new = derive(old)
            if len(new) != len(old) or list(new.columns)[:len(old.columns)] != [c for c in old.columns if c not in NEW]:
                sys.exit(f'{t}/{key}: filas o columnas distintas de lo servido')
            p = os.path.join(stage, 'sat_dpto', dname)
            new.to_parquet(p, index=False)
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
        if terr_radios:
            gap = 100 * (terr_cells - terr_radios) / terr_radios
            print(f'{t}: Σ ocupados_est {terr_cells:,.0f} vs Σ ocupados_n {terr_radios:,} ({gap:+.1f} %)')
            if abs(gap) > TOL_OCUP_TERR:
                problems.append(f'{t}: Σ ocupados_est {gap:+.1f} % contra los radios')

    if notes:
        print('Departamentos cuyas celdas suman otra población que sus radios (aviso):\n  ' + '\n  '.join(notes))
    print(f'\n{len(staged)} archivos preparados en {os.path.join(OUTPUT_DIR, "econ_hex_derived")}')
    if problems:
        print('Controles fuera de tolerancia:\n  ' + '\n  '.join(problems))
        sys.exit(1)
    print('Controles OK (Σ ocupados por depto y desocupación ponderada contra el resumen).')
    if args.dry_run:
        return
    from upload_to_r2 import upload_file
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{len(failed)} subidas fallaron: {failed[:5]}')
    print('Publicado. Falta: DEPT_V y ?v en config.ts, npm run purge-cdn.')


if __name__ == '__main__':
    main()
