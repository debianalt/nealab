"""
Phase 2 orchestrator (raster/SDM/Overture) for any non-Misiones territory.

Generalizes + extends corrientes_pipeline_runner.py (proven Corrientes logic)
to a single, territory-parametrized driver covering ALL Phase 2 analyses:

  carbon_stock, pm25_drivers, productive_activity, deforestation_dynamics,
  land_use, accessibility, soil_water, forestry_aptitude, flood_risk,
  climate_vulnerability, territorial_scores

Mirrors exactly how Itapúa/Corrientes were done:
  - GEE exports forced to GCS (--gcs); pipeline downloads from GCS.
  - Scoring uses goalpost normalization (--mode comparable) wherever the
    script supports it, so scores stay cross-territory comparable.
  - R2 keys ALWAYS under neahub/data/<output_prefix>/ (the orphan-path bug
    that bit run_itapua_pipeline.py is avoided here by construction).
  - npx/gcloud invoked via the Windows .cmd paths + shell=True (same as the
    Corrientes runner) so it runs locally on this machine.

Flow:
  Wave A  submit all GEE export jobs (async)
  Poll loop: as rasters land in GCS, download → process (--mode comparable)
             → split_by_admin → upload to R2 data/<prefix>/
  territorial_scores (no GEE) + flood_risk (self-contained) run alongside
  climate_vulnerability runs last (depends on climate_comfort + flood)

Usage:
  python pipeline/run_phase2_pipeline.py --territory alto_parana_py
  python pipeline/run_phase2_pipeline.py --territory alto_parana_py --dry-run
  python pipeline/run_phase2_pipeline.py --territory alto_parana_py --skip-submit
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, R2_BUCKET, get_territory

# Windows .cmd paths (same as corrientes_pipeline_runner.py — proven on this box)
GCLOUD = (r'C:\Users\ant\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd'
          if sys.platform == 'win32' else 'gcloud')
NPX = r'C:\Program Files\nodejs\npx.cmd' if sys.platform == 'win32' else 'npx'
_SHELL = sys.platform == 'win32'

DRY = False


def sh(cmd_list, **kw):
    print(f"  $ {' '.join(str(c) for c in cmd_list)}")
    if DRY:
        return 0
    return subprocess.run(cmd_list, cwd=ROOT_DIR, **kw).returncode


def py(script, *args):
    return sh([sys.executable, os.path.join(SCRIPT_DIR, script), *args]) == 0


def gee_init():
    import ee
    key_env = os.environ.get('GEE_SERVICE_ACCOUNT_KEY', '')
    if key_env and not os.path.isfile(key_env):
        kd = json.loads(key_env)
        ee.Initialize(ee.ServiceAccountCredentials(kd['client_email'], key_data=kd))
    else:
        ee.Initialize()
    return ee


def gee_done(ee):
    tasks = ee.data.getTaskList()
    return {t['description'] for t in tasks if t['state'] == 'COMPLETED'}


def gcs_dl(src_url, dest_dir):
    print(f"  DL {os.path.basename(src_url)}")
    if DRY:
        return True
    r = subprocess.run([GCLOUD, 'storage', 'cp', src_url, dest_dir],
                        cwd=ROOT_DIR, capture_output=True, text=True, shell=_SHELL)
    if r.returncode != 0:
        print(f"  FAIL dl: {r.stderr.strip()[-200:]}")
        return False
    return True


def r2_put(local_path, r2_key):
    """r2_key MUST start with data/ — public bucket serves from /data/."""
    assert r2_key.startswith('data/'), f"R2 key missing data/ prefix: {r2_key}"
    print(f"  R2 put {R2_BUCKET}/{r2_key}")
    if DRY:
        return True
    r = subprocess.run(
        [NPX, 'wrangler', 'r2', 'object', 'put', f'{R2_BUCKET}/{r2_key}',
         '--file', local_path, '--remote'],
        cwd=ROOT_DIR, capture_output=True, text=True, shell=_SHELL)
    if r.returncode != 0:
        print(f"  R2 FAIL {r2_key}: {r.stderr[-200:]}")
        return False
    return True


def split_and_upload(territory_id, t_dir, out_prefix, analysis_id):
    if not py('split_by_admin.py', '--territory', territory_id, '--only', analysis_id):
        print(f"  split FAILED {analysis_id}")
        return False
    gp = os.path.join(t_dir, f'sat_{analysis_id}.parquet')
    if os.path.exists(gp) or DRY:
        r2_put(gp, f'data/{out_prefix}sat_{analysis_id}.parquet')
    dpto_dir = os.path.join(t_dir, 'sat_dpto')
    files = glob.glob(os.path.join(dpto_dir, f'sat_{analysis_id}_*.parquet'))
    n = 0
    for fp in files:
        if r2_put(fp, f'data/{out_prefix}sat_dpto/{os.path.basename(fp)}'):
            n += 1
    print(f"  {analysis_id}: uploaded {n}/{len(files)} dpto parquets")
    return True


# ── Wave A: submit all GEE export jobs (async, --gcs) ────────────────────────
def submit_exports(tid):
    print("\n" + "=" * 60 + "\n  WAVE A: submit GEE exports (--gcs, async)\n" + "=" * 60)
    py('gee_export_carbon_stock.py', '--territory', tid, '--gcs', '--no-wait')
    py('gee_export_pm25_annual.py', '--territory', tid, '--gcs', '--no-wait')
    py('gee_export_activity_rasters.py', '--territory', tid, '--gcs')
    py('gee_export_hansen_loss.py', '--territory', tid, '--gcs', '--no-wait')
    py('gee_export_location_value.py', '--territory', tid, '--gcs')
    py('gee_export_soil_water.py', '--territory', tid)
    py('gee_export_sdm_covariates.py', '--territory', tid)
    # flood_risk is self-contained (own GEE submit + R2 upload)
    print("  Submitted. flood_risk runs via run_flood_update separately.")


def main():
    global DRY
    ap = argparse.ArgumentParser(description="Phase 2 orchestrator (any territory)")
    ap.add_argument('--territory', required=True)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--skip-submit', action='store_true',
                    help='GEE exports already submitted/completed')
    ap.add_argument('--poll-seconds', type=int, default=90)
    args = ap.parse_args()
    DRY = args.dry_run

    territory = get_territory(args.territory)
    tid = territory['id']
    out_prefix = territory['output_prefix']            # 'alto_parana_py/'
    t_dir = os.path.join(OUTPUT_DIR, out_prefix.rstrip('/'))
    gcs = f"gs://{GCS_BUCKET}/satellite/{tid}"

    print("=" * 60)
    print(f"  Phase 2 — {territory['label']} ({tid})  dry_run={DRY}")
    print(f"  out_dir={t_dir}  r2=data/{out_prefix}")
    print("=" * 60)

    # territorial_scores: no GEE, run immediately (DuckDB/Overture)
    print("\n-- territorial_scores (Overture, no GEE) --")
    if py('ingest_overture.py', '--territory', tid) and \
       py('compute_overture_scores.py', '--territory', tid, '--mode', 'comparable'):
        py('split_scores_by_dpto.py', '--territory', tid)
        print("  territorial_scores DONE (self-split/upload)")

    # flood_risk: self-contained (GEE→download→H3→split→R2)
    print("\n-- flood_risk (run_flood_update, self-contained) --")
    py('run_flood_update.py', '--territory', tid)

    if not args.skip_submit:
        submit_exports(tid)

    ee = gee_init() if not DRY else None
    state = {k: False for k in [
        'carbon_stock', 'pm25_drivers', 'productive_activity',
        'deforestation_dynamics', 'soil_water', 'accessibility',
        'land_use', 'forestry_aptitude', 'climate_vulnerability']}

    def done_file(name):
        return os.path.exists(os.path.join(t_dir, name))

    while not all(state.values()):
        completed = gee_done(ee) if ee else set()
        print(f"\n[{time.strftime('%H:%M:%S')}] pending: {[k for k,v in state.items() if not v]}")

        # carbon_stock
        if not state['carbon_stock']:
            r = os.path.join(t_dir, 'sat_carbon_stock_raster.tif')
            if not os.path.exists(r):
                gcs_dl(f'{gcs}/sat_carbon_stock_raster.tif', t_dir)
            if os.path.exists(r) or DRY:
                if py('process_carbon_to_h3.py', '--territory', tid, '--mode', 'comparable') \
                   and (done_file('sat_carbon_stock.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'carbon_stock')
                    state['carbon_stock'] = True

        # soil_water (always GCS, soil_water/ subdir)
        if not state['soil_water']:
            r = os.path.join(t_dir, 'sat_soil_water_raster.tif')
            if not os.path.exists(r):
                gcs_dl(f'gs://{GCS_BUCKET}/soil_water/{tid}/sat_soil_water_raster.tif', t_dir)
            if os.path.exists(r) or DRY:
                if py('process_raster_to_h3.py', '--territory', tid,
                      '--analysis', 'soil_water', '--mode', 'comparable') \
                   and (done_file('sat_soil_water.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'soil_water')
                    state['soil_water'] = True

        # deforestation_dynamics (Hansen pair)
        if not state['deforestation_dynamics']:
            need = ['hansen_lossyear.tif', 'hansen_treecover2000.tif']
            for nm in need:
                if not os.path.exists(os.path.join(t_dir, nm)):
                    gcs_dl(f'{gcs}/{nm}', t_dir)
            if all(os.path.exists(os.path.join(t_dir, nm)) for nm in need) or DRY:
                if py('process_hansen_to_h3.py', '--territory', tid, '--mode', 'comparable') \
                   and (done_file('sat_deforestation_dynamics.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'deforestation_dynamics')
                    state['deforestation_dynamics'] = True

        # productive_activity (raster path, NOT compute_productive_activity.py)
        if not state['productive_activity']:
            r = os.path.join(t_dir, 'sat_activity_raster.tif')
            if not os.path.exists(r):
                gcs_dl(f'{gcs}/sat_activity_raster.tif', t_dir)
            if os.path.exists(r) or DRY:
                if py('process_activity_to_h3.py', '--territory', tid, '--mode', 'comparable') \
                   and (done_file('sat_productive_activity.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'productive_activity')
                    state['productive_activity'] = True

        # accessibility (lv_friction + lv_cities_access)
        if not state['accessibility']:
            for nm in ['lv_friction.tif', 'lv_cities_access.tif']:
                if not os.path.exists(os.path.join(t_dir, nm)):
                    gcs_dl(f'{gcs}/{nm}', t_dir)
            if all(os.path.exists(os.path.join(t_dir, nm))
                   for nm in ['lv_friction.tif', 'lv_cities_access.tif']) or DRY:
                if py('compute_accessibility_h3.py', '--territory', tid, '--mode', 'comparable') \
                   and (done_file('sat_accessibility.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'accessibility')
                    state['accessibility'] = True

        # pm25_drivers (25 annual rasters)
        if not state['pm25_drivers']:
            for y in range(1998, 2023):
                f = os.path.join(t_dir, f'sat_pm25_{y}.tif')
                if not os.path.exists(f) and f'{tid}_sat_pm25_{y}' in completed:
                    gcs_dl(f'{gcs}/sat_pm25_{y}.tif', t_dir)
            have = all(os.path.exists(os.path.join(t_dir, f'sat_pm25_{y}.tif'))
                       for y in range(1998, 2023))
            if have or DRY:
                if py('process_pm25_annual_to_h3.py', '--territory', tid) \
                   and py('compute_pm25_drivers.py', '--territory', tid, '--mode', 'comparable') \
                   and (done_file('sat_pm25_drivers.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'pm25_drivers')
                    state['pm25_drivers'] = True

        # SDM covariates → land_use + forestry_aptitude
        sdm_ready = done_file('sdm_mapbiomas_py.tif') or \
            glob.glob(os.path.join(t_dir, 'sdm_*.tif'))
        if not state['land_use']:
            if not sdm_ready and not DRY:
                py('process_sdm_covariates_h3.py', '--territory', tid)
                sdm_ready = done_file('sdm_mapbiomas_py.tif')
            if sdm_ready or DRY:
                if py('process_mapbiomas_to_h3.py', '--territory', tid) \
                   and (done_file('sat_land_use.parquet') or DRY):
                    split_and_upload(tid, t_dir, out_prefix, 'land_use')
                    state['land_use'] = True
        if not state['forestry_aptitude'] and (sdm_ready or DRY):
            if py('compute_forestry_sdm.py', '--territory', tid) \
               and (done_file('sat_forestry_aptitude.parquet') or DRY):
                split_and_upload(tid, t_dir, out_prefix, 'forestry_aptitude')
                state['forestry_aptitude'] = True

        # climate_vulnerability (depends on climate_comfort[P1] + flood)
        if not state['climate_vulnerability'] and done_file('hex_flood_risk.parquet'):
            if py('compute_climate_vulnerability.py', '--territory', tid, '--mode', 'comparable') \
               and (done_file('sat_climate_vulnerability.parquet') or DRY):
                split_and_upload(tid, t_dir, out_prefix, 'climate_vulnerability')
                state['climate_vulnerability'] = True

        if DRY:
            print("  [dry-run] stopping after one pass")
            break
        if all(state.values()):
            break
        time.sleep(args.poll_seconds)

    print("\n" + "=" * 60)
    print(f"  Phase 2 {territory['label']}: {sum(state.values())}/{len(state)} done")
    print(f"  state: {state}")
    print("  Next: flip config.ts coverage pending->available for done analyses,")
    print("        wire deptSummaries, bump cache-busters, deploy.")
    print("=" * 60)
    return 0 if all(state.values()) else 1


if __name__ == '__main__':
    sys.exit(main())
