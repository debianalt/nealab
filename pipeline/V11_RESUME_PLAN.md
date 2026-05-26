# v1.1 Re-baseline — Plan de retoma (sesión 24-may-2026 noche)

## Estado al cerrar sesión

**Hecho y persistido**:
- `pipeline/compute_goalposts_v11.py` — script nuevo, lee rasters multi-band directos (no parquets bugueados). 9 territorios pool, subsample 200K pix/(territory, componente) para balance.
- `pipeline/config/goalposts.json` — **v1.1 escrito** (backup v1.0 en `goalposts_v1.0.json.bak` + `goalposts.json.bak.20260524_233523`).
- `pipeline/scoring.py:276` — fix encoding UTF-8 en `load_goalposts()` (necesario porque goalposts.json v1.1 tiene tildes/Atlântica).
- `pipeline/fanout_v11_reprocess.sh` — script idempotent (skip parquets con mtime > goalposts.json).
- **5/54 parquets v1.1 reescritos**:
  - misiones: climate_comfort (smoke test), environmental_risk, green_capital, change_pressure, agri_potential
- Output logs: `pipeline/output/v11_fanout_logs/<territory>_<analysis>.log`
- Dry-run + diff log: `pipeline/output/goalposts_v11_rasterdirect_dryrun.log`

**Bug encontrado y rationale del fix**:
- `process_raster_to_h3.py:147` aplica `series.rank(pct=True) * 100` antes de guardar. Los parquets tienen `c_*` como percentile uniform (0-100), NO raw.
- `compute_goalposts.py` (script viejo) pooleaba esos percentiles → P2/P98 = [~2, ~98] basura.
- Los goalposts.json v1.0 actuales fueron computados con datos raw de otro lado (manual o script viejo desconocido).
- Fix elegido (Opción B-bypass del advisor): leer rasters multi-band directos (`sat_<analysis>_raster.tif`), pool raw pixels, P2/P98 sobre raw. NO se tocó `process_raster_to_h3.py` ni el formato de parquets.

## Pendientes para mañana

### 1. Reanudar fan-out (49 jobs restantes)

```bash
cd /c/Users/ant/OneDrive/nealab/neahub
./pipeline/fanout_v11_reprocess.sh
```

Script es idempotent: detecta los 5 ya hechos y solo procesa los 49 restantes. ETA ~5-6h con 4 workers paralelos. Si querés más paralelismo, editar `xargs -P 4` a `-P 6` (cuidado memoria con rasters BR grandes).

**Lista exacta de 49 pendientes** (territory, analysis):
```
misiones forest_health
corrientes {environmental_risk, climate_comfort, green_capital, change_pressure, agri_potential, forest_health}
itapua_py {las 6}
alto_parana_py {las 6}
chaco {las 6}
formosa {las 6}
parana_br {las 6}
santa_catarina_br {las 6}
rio_grande_sul_br {las 6}
```

### 2. Verificación post-fanout

```bash
python -c "
import pandas as pd
for t in ['rio_grande_sul_br', 'parana_br', 'chaco']:
    df = pd.read_parquet(f'pipeline/output/{t}/sat_climate_comfort.parquet')
    print(t, 'score stats:', df['score'].describe()[['min','50%','max']].to_dict())
    print(t, 'saturation 0/100:',
          (df['score']==0).mean()*100, (df['score']==100).mean()*100)
"
```

**Criterio aceptación**: scores no uniformemente distribuidos, std razonable (>5), saturación 0 o 100 <10% por capa. Si alguna capa satura >20%, revisar el goalpost de esa capa (ej. `c_deforest hi=1.0` puede ser muy estricto para Misiones que tiene tasa 0%).

### 3. Split by admin (9 territorios × 6 capas)

```bash
for t in misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br; do
    python pipeline/split_by_admin.py --territory $t \
        --only environmental_risk,climate_comfort,green_capital,change_pressure,agri_potential,forest_health
done
```

### 4. R2 upload (CRITICAL: usar --remote y prefix data/)

```bash
for t in misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br; do
    prefix=""
    [ "$t" != "misiones" ] && prefix="$t/"
    dir="pipeline/output"
    [ "$t" != "misiones" ] && dir="pipeline/output/$t"
    for a in environmental_risk climate_comfort green_capital change_pressure agri_potential forest_health; do
        f="$dir/sat_${a}.parquet"
        [ -f "$f" ] && npx wrangler r2 object put "neahub/data/${prefix}sat_${a}.parquet" --file "$f" --remote
    done
    # sat_dpto subdirectory
    dpto="$dir/sat_dpto"
    if [ -d "$dpto" ]; then
        for f in "$dpto"/*.parquet; do
            base=$(basename "$f")
            npx wrangler r2 object put "neahub/data/${prefix}sat_dpto/${base}" --file "$f" --remote
        done
    fi
done
```

### 5. Frontend updates (`src/lib/config.ts`)

Cache busters — bump cada uno de los 6 core (ej. `?v=N+1`):
- `sat_environmental_risk`
- `sat_climate_comfort`
- `sat_green_capital`
- `sat_change_pressure`
- `sat_agri_potential`
- `sat_forest_health`

ANALYSIS_REGISTRY coverage — agregar para las 6 capas core:
```js
coverage: {
    ...existing,
    chaco: 'available',
    formosa: 'available',
    parana_br: 'available',
    santa_catarina_br: 'available',
    rio_grande_sul_br: 'available',
}
```

### 6. Deploy

```bash
git add -A && git commit -m "feat(v1.1): goalpost re-baseline pool 9 territorios (NEA + BR sur)"
git push origin master:main
npm run deploy
```

### 7. Documentar bump (opcional pero importante)

- CITATION.cff: version 1.2.0 + bump Zenodo release
- Update CLAUDE.md neahub: anotar que v1.1 está live, pool = 9 territorios

## Diferidos (FUERA de v1.1)

- **5 capas specialized** (carbon_stock, productive_activity, deforestation_dynamics, pm25_drivers, location_value) para los 5 nuevos territorios. Rasters están en local (`act_*.tif`, `hansen_*.tif`, `sat_pm25_*.tif`, `sat_carbon_stock_raster.tif`). Necesitan scripts dedicados de processing (no `process_raster_to_h3`). Sus goalposts en v1.1 mantienen v1.0 (pool=4 sin cambios). Fase B.
- **land_use** para los 5 nuevos. AR (chaco/formosa) trivial con `gee_export_mapbiomas.py` existente. BR (parana_br/SC/RS) requiere MapBiomas Brasil (asset distinto + remap diferente + script nuevo). NO está en `COMPARABLE_PARQUETS` de `compute_goalposts.py` — no afecta v1.1.

## Diff v1.0 → v1.1 (referencia)

21 indicadores Tier 2 actualizados, 0 PCA selections cambiadas (todos componentes retenidos). Highlights:
- `c_heat_day`: [15, 50] → [20.7, 31.3]°C (era placeholder, ahora empírico)
- `c_precipitation`: [1444, 1961] → [615.5, 1940] mm/yr (lo baja por Chaco semi-árido)
- `c_deforest`/`c_hansen_loss`: hi cae a ~1% (P98 real es bajo, hot-spots se destacan)
- `c_ndvi`: [0.05, 0.9] → [0.264, 0.869] (lo más realista para vegetación tropical)
- 3 indicadores NO actualizados (c_fire, c_fire_count, c_frost): P98 = 0 (sparse/discrete) → goalpost v1.0 preservado

Ver log completo: `pipeline/output/goalposts_v11_rasterdirect_dryrun.log`

## Rollback (si algo sale mal)

```bash
# Restaurar goalposts v1.0
cp pipeline/config/goalposts_v1.0.json.bak pipeline/config/goalposts.json
# Restaurar parquets v1.0: re-correr process_raster_to_h3.py --mode local
#   (los parquets actuales con mtime > 23:35 son v1.1; re-procesarlos en local mode los vuelve al estado v1.0)
```
