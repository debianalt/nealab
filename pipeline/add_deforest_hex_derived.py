"""
Pérdida como % del bosque por hexágono en deforestation_dynamics (5-nov-2026).

La pestaña «% del bosque» del ranking (build_deforest_rank.py: Σ pérdida / Σ cobertura
arbórea 2000 del departamento) no tenía columna por celda: con un departamento abierto el
mapa pintaba el departamento entero de un gris plano. La lectura por celda sale de columnas
que cada celda ya tiene (Hansen GFC v1.13):

    c_loss_pct_forest          = 100 · c_loss_rate / c_treecover            (2016–2025)
    c_loss_pct_forest_baseline = 100 · c_loss_rate_baseline / c_treecover   (2001–2010)
    c_loss_pct_forest_delta    = c_loss_pct_forest − c_loss_pct_forest_baseline

c_loss_rate = % de la celda perdido por año; c_treecover = % de la celda con cobertura
arbórea en 2000 (el mismo denominador del ranking, también para la línea de base). Sólo
donde c_treecover ≥ MIN_TREECOVER: con menos, unas decenas de píxeles harían «100 % del
bosque»; esas celdas quedan vacías («sin bosque en 2000»). El trío _baseline/_delta deja
funcionar el selector temporal sobre esta pestaña (getTemporalCol en src/lib/config.ts).

FUENTE: lo ÚLTIMO PUBLICADO, bajado del CDN a una carpeta NUEVA (--fetch-source la arma; la
trampa de add_econ_hex_derived.py del 5-nov fue tomar un respaldo viejo como fuente). El
script sólo agrega tres columnas al final: cada archivo tiene que conservar filas, h3index y
todas las columnas de la fuente con los mismos valores, o frena.

    python pipeline/add_deforest_hex_derived.py --source-dir <nueva> --fetch-source --dry-run
    python pipeline/add_deforest_hex_derived.py --source-dir <la misma> --backup-dir C:/Users/ant/r2-deforest-backup-2026-11-05

Después: `npm run purge-cdn`, subir el ?v de sat_deforestation_dynamics y DEPT_V en
src/lib/config.ts (+ deploy).
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

LAYER = 'deforestation_dynamics'
TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa', 'itapua_py', 'alto_parana_py']
NEW = ['c_loss_pct_forest', 'c_loss_pct_forest_baseline', 'c_loss_pct_forest_delta']
REQUIRED = ['c_loss_rate', 'c_loss_rate_baseline', 'c_loss_rate_delta', 'c_treecover']
MIN_TREECOVER = 10.0    # % of the cell with tree cover in 2000
TOL_RANK_PTS = 0.05     # department Σ loss / Σ forest vs the summary's loss_pct_forest


def summary_path(t: str) -> str:
    return os.path.join(SRC_DATA_DIR, f'sat_{LAYER}_dept_summary.json' if t == 'misiones'
                        else f'{t}_sat_{LAYER}_summary.json')


def derive(df: pd.DataFrame) -> pd.DataFrame:
    tc = df['c_treecover']
    ok = tc.notna() & (tc >= MIN_TREECOVER)
    out = df.drop(columns=[c for c in NEW if c in df.columns])
    cur = np.where(ok & df['c_loss_rate'].notna(), 100 * df['c_loss_rate'] / tc, np.nan)
    base = np.where(ok & df['c_loss_rate_baseline'].notna(), 100 * df['c_loss_rate_baseline'] / tc, np.nan)
    out['c_loss_pct_forest'] = np.round(cur, 3)
    out['c_loss_pct_forest_baseline'] = np.round(base, 3)
    out['c_loss_pct_forest_delta'] = np.round(cur - base, 3)
    return out


def checked(src: pd.DataFrame, label: str) -> pd.DataFrame:
    """Derive and prove nothing but the three new columns changed."""
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


def fetch_source(source_dir: str, territories: list[str]) -> None:
    if os.path.exists(source_dir):
        sys.exit(f'{source_dir} ya existe: --fetch-source baja a una carpeta nueva')
    for t in territories:
        prefix = get_territory(t)['output_prefix']
        with open(summary_path(t), encoding='utf-8') as f:
            summary = json.load(f)
        d = os.path.join(source_dir, t, 'sat_dpto')
        os.makedirs(d, exist_ok=True)
        fetch(f'{CDN}/{prefix}sat_{LAYER}.parquet', os.path.join(source_dir, t, f'sat_{LAYER}.parquet'))
        for dep in summary['departments']:
            name = f'sat_{LAYER}_{dep["parquetKey"]}.parquet'
            fetch(f'{CDN}/{prefix}sat_dpto/{name}', os.path.join(d, name))
        print(f'{t}: fuente bajada ({len(summary["departments"])} deptos)')


def main() -> None:
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser()
    ap.add_argument('--source-dir', required=True)
    ap.add_argument('--fetch-source', action='store_true', help='bajar lo publicado a --source-dir (carpeta nueva)')
    ap.add_argument('--backup-dir', help='carpeta NUEVA para respaldar lo que hoy sirve R2 (obligatoria al subir)')
    ap.add_argument('--territory', choices=TERRITORIES, action='append')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    territories = args.territory or TERRITORIES
    if not args.dry_run:
        if not args.backup_dir:
            sys.exit('--backup-dir es obligatorio al subir')
        if os.path.exists(args.backup_dir):
            sys.exit(f'{args.backup_dir} ya existe: usar una carpeta nueva')
    if args.fetch_source:
        fetch_source(args.source_dir, territories)

    staged, problems = [], []
    for t in territories:
        prefix = get_territory(t)['output_prefix']
        with open(summary_path(t), encoding='utf-8') as f:
            summary = json.load(f)
        src_dir = os.path.join(args.source_dir, t)
        stage = os.path.join(OUTPUT_DIR, 'deforest_hex_derived', t)
        os.makedirs(os.path.join(stage, 'sat_dpto'), exist_ok=True)

        name = f'sat_{LAYER}.parquet'
        d = checked(pd.read_parquet(os.path.join(src_dir, name)), f'{t}/territorio')
        p = os.path.join(stage, name)
        d.to_parquet(p, index=False, compression='zstd')
        staged.append((p, f'data/{prefix}{name}'))
        v = d.c_loss_pct_forest
        delta_gap = float(np.nanmax(np.abs(d.c_loss_rate_delta - (d.c_loss_rate - d.c_loss_rate_baseline))))
        print(f'\n{t}: {len(d):,} celdas · con bosque ≥ {MIN_TREECOVER:.0f} % {v.notna().sum():,} '
              f'· % del bosque/año P50/P90/P99/máx {np.nanpercentile(v, [50, 90, 99]).round(2).tolist()} / {np.nanmax(v):.1f}'
              f' · |delta publicado − (actual − base)| máx {delta_gap:.3f}')

        dept_rows = 0
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
            # The ranking's department figure, rebuilt from its cells (build_deforest_rank.py).
            r = (dep.get('rank') or {}).get('loss_pct_forest')
            forest = new.c_treecover.fillna(0).sum()
            if r is not None and forest > 0:
                mine = 100 * new.c_loss_rate.fillna(0).sum() / forest
                if abs(mine - r) > TOL_RANK_PTS:
                    problems.append(f'{t}/{dep.get("dpto") or key}: Σ pérdida/Σ bosque {mine:.2f} vs ranking {r}')
        # Not a control: the published department files carry the full grid of their
        # department (Corrientes 1.03 M rows) and the territory file only the cells with data.
        print(f'{t}: {len(summary["departments"])} deptos, {dept_rows:,} filas (territorio {len(d):,})')

    print(f'\n{len(staged)} archivos preparados en {os.path.join(OUTPUT_DIR, "deforest_hex_derived")}')
    if problems:
        print('Controles fuera de tolerancia:\n  ' + '\n  '.join(problems))
        sys.exit(1)
    print('Controles OK: fuente intacta + 3 columnas; % del bosque por depto = ranking.')
    if args.dry_run:
        return

    for p, key in staged:
        dest = os.path.join(args.backup_dir, key[len('data/'):])
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        fetch(f'{CDN}/{key[len("data/"):]}', dest)
    print(f'Respaldo de lo servido: {args.backup_dir} ({len(staged)} archivos)')
    from upload_to_r2 import upload_file
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{len(failed)} subidas fallaron: {failed[:5]}')
    print('Publicado. Falta: npm run purge-cdn, ?v de sat_deforestation_dynamics y DEPT_V + deploy.')


if __name__ == '__main__':
    main()
