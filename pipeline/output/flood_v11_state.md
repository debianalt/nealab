# Flood pipeline v1.1 — state checkpoint (2026-05-28)

25 GEE tasks launched for the 5 v1.1 territories to populate flood_risk.
GEE compute runs on Google's side; this file lists the IDs and the
follow-up steps so a later session can pick up.

## GEE task IDs

Monitor: https://code.earthengine.google.com/tasks

### S1 historical recurrence + current extent (10 tasks)
Launched by `run_flood_update.py --territory=<t> --historical`.

| Territory | Recurrence task | Current task |
|---|---|---|
| chaco | PRR5GZUSSI56JBUWN5BIDIZX | SHVMYVJF5BKJGRGRENX6C22U |
| chaco (orphan) | — | K6UMQYF6J6BAPTR3RC2DZXCY (first --current attempt, stopped) |
| formosa | QOHW4RUS3FHRISGIY42XBHCA | VW5FJJVMRWI22CGTTUEGURZ5 |
| parana_br | S3EHDUR6KO2326WHNZUMMNBB | BXZSAR2O77UAM3L3E5QNXH6U |
| santa_catarina_br | VV65Z3TOJIZUYMRRAEG6YFWK | U4MLGHBCNWLZTPEUBGNXZ6UD |
| rio_grande_sul_br | UHDQAHJPALJBRKBHR7PUTCCB | QC7H7UF65GXOKWDIWBPFM3PG |

### JRC Global Surface Water v1.4 (15 tasks)
Launched by `pipeline/launch_flood_jrc.py` (new in this session — the
orchestrator doesn't expose `--jrc`).

| Territory | Occurrence | Recurrence | Seasonality |
|---|---|---|---|
| chaco | 723PLEFDG2EMCLUQGHYGZISR | W476FUHGUTN5BRZWDE6WTQAU | 7Y2NEW7HCN2CXGIL6NGOJFJO |
| formosa | RMXU5XZPTIBSYFFHYNT265RN | BJ6L6IDQVTRBDQQRULHEZ4QF | E3HQQSI2XWIMABJTGOWLSQPI |
| parana_br | TZTQHV6HZLN2QJNNDDAHP3SG | JJXB3GJXSS42PGCFPZXEAHSP | AKSSKWNZCVIDYAAXGH3VL525 |
| santa_catarina_br | N77QO7MKSDVQW3D64DZ7YX4X | 3OGZZMPEFXRUNYS3ATSCOAZN | 2XFMDMYOMDPGZGN37X5W3UGQ |
| rio_grande_sul_br | HOCYIMQ73GN7ADBY7UCDLNCL | S6VIN676KHWRYV47GDTUW6WI | UV4ISOSQ5FRNEDLP6HFAXLZT |

ETA: 4-6 hours wall time. GCS output: `gs://spatia-satellite/flood/`.

## Pipeline gaps discovered (not yet fixed)

1. **`run_flood_update.py` doesn't expose `--jrc`**. Without it,
   `launch_exports` is called with `jrc=False` → no JRC tifs. The Misiones
   flood was generated with JRC files pre-downloaded ages ago. For new
   territories, we need a separate launch (now in `launch_flood_jrc.py`).
   *Proper fix: add `--jrc` flag to the orchestrator and pass through.*

2. **`download_latest_flood(OUTPUT_DIR)` ignores `t_dir`**. The download
   step writes to the root output dir regardless of `--territory`. Process
   step then reads from `t_dir` and won't find anything. *Proper fix:
   pass `t_dir` (or `output_dir=t_dir`) when calling download_latest_flood.*

## Follow-up steps when GEE tasks finish

For each territory `<t>` in {chaco, formosa, parana_br, santa_catarina_br,
rio_grande_sul_br}:

```bash
# 1) Download tifs from GCS into the territory's output dir
gcloud storage cp gs://spatia-satellite/flood/flood_recurrence_historical*.tif pipeline/output/<t>/
gcloud storage cp gs://spatia-satellite/flood/flood_current_*.tif pipeline/output/<t>/
gcloud storage cp gs://spatia-satellite/flood/jrc_occurrence*.tif pipeline/output/<t>/
gcloud storage cp gs://spatia-satellite/flood/jrc_recurrence*.tif pipeline/output/<t>/
gcloud storage cp gs://spatia-satellite/flood/jrc_seasonality*.tif pipeline/output/<t>/

# (the gcloud paths above are illustrative — confirm exact names in GCS,
#  which include territory-specific suffixes when present)

# 2) Process with --skip-gee
python pipeline/run_flood_update.py --territory=<t> --skip-gee

# 3) Generate dept summary JSON (mirrors itapua/AP pattern; see other
#    sat_*_dept_summary files for shape)
python pipeline/split_flood_by_dpto.py --territory=<t>   # script may need creation

# 4) Bundle the summary JSON into src/lib/data/<t>_flood_dept_summary.json
cp pipeline/output/<t>/<t>_flood_dept_summary.json src/lib/data/

# 5) Add the bundled JSON to deptSummaries.ts under the relevant
#    TERRITORY_SUMMARIES map (chaco, formosa, parana_br, etc.)

# 6) Flip coverage in src/lib/config.ts: change
#    `flood_risk: { ..., <t>: 'unavailable' }` to `'available'` for each
#    territory that landed cleanly.

# 7) Commit + push + deploy.
```

## Notes for the follow-up session

- The orphan chaco S1 current task (K6UMQYF6J6BAPTR3RC2DZXCY) will produce
  a tif with a slightly earlier timestamp than the second attempt; either
  works, just pick one.
- BR municipios may not align with INDEC-radio-based "department" stats
  that the Misiones flood pipeline assumes. Verify split_by_admin uses
  the GADM admin-2 crosswalk already generated for v1.1.
- After deploy, expected to flip `coverage.flood_risk` for the 5 new
  territories from `'unavailable'` to `'available'` in `src/lib/config.ts`.
