import { query, isReady } from '$lib/stores/duckdb';
import { PARQUETS } from '$lib/config';

export const PETAL_VARS = [
	{ col: 'tasa_actividad', labelKey: 'label.activityRate' },
	{ col: 'tasa_empleo', labelKey: 'label.employmentRate' },
	{ col: 'pct_universitario', labelKey: 'label.university' },
	{ col: 'pct_nbi', labelKey: 'label.ubn' },
	{ col: 'pct_hacinamiento', labelKey: 'label.overcrowding' },
	{ col: 'pct_agua_red', labelKey: 'label.waterNetwork' },
];

// IBGE Censo 2022 setor-level variables (Brazil). Mode = local, not comparable to AR/PY.
export const PETAL_VARS_BR = [
	{ col: 'pct_agua_rede',       labelKey: 'label.br.waterNetwork' },
	{ col: 'pct_esgoto_adequado', labelKey: 'label.br.sewerNetwork' },
	{ col: 'pct_lixo_coletado',   labelKey: 'label.br.garbageCollected' },
	{ col: 'pct_alfabetizado',    labelKey: 'label.br.literacy' },
	{ col: 'pct_sem_banheiro',    labelKey: 'label.br.noBathroom' },
	{ col: 'densidad_hab_km2',    labelKey: 'radio.densidad' },
];

/** (value / provincialAvg) * 50, clamped [0, 100]. 50 = provincial average. */
export function normalizeValues(rawValues: number[], provAvg: number[]): number[] {
	return rawValues.map((v, i) => {
		const avg = provAvg[i];
		if (avg === 0) return 50;
		return Math.min(100, Math.max(0, (v / avg) * 50));
	});
}

const _cachedProvAvg = new Map<string, number[]>();

/** Map a radio/setor code to its Spatia territory id via UF/codprov prefix. */
export function territoryFromRedcode(rc: string): string {
	const p = String(rc).slice(0, 2);
	// Brazil (IBGE UF codes, 15-digit cd_setor)
	if (p === '41') return 'parana_br';
	if (p === '42') return 'santa_catarina_br';
	if (p === '43') return 'rio_grande_sul_br';
	// Argentina (INDEC codprov, 9-digit redcode)
	return p === '18' ? 'corrientes'
	     : p === '22' ? 'chaco'
	     : p === '34' ? 'formosa'
	     :              'misiones';  // '54' and fallback
}

/**
 * Population-weighted provincial averages aligned to PETAL_VARS, for the given
 * territory (default Misiones). Vars absent from a territory's census (e.g.
 * pct_agua_red in Corrientes/Chaco/Formosa) get 1.0 — their raw value is 0 there
 * too, so the petal axis reads ~0. Cached per territory. Anchoring each zone to
 * its OWN province makes cross-territory zone petals comparable.
 */
export async function getProvincialAvg(territory: string = 'misiones'): Promise<number[]> {
	const cached = _cachedProvAvg.get(territory);
	if (cached) return cached;
	if (!isReady()) throw new Error('DuckDB not ready');

	const src = CENSUS_SRC[territory] ?? CENSUS_SRC.misiones;
	const present = new Set(src.vars.map(v => v.col));
	const cols = PETAL_VARS.filter(v => present.has(v.col))
		.map(v => `SUM(${v.col} * total_personas) / NULLIF(SUM(total_personas), 0) as avg_${v.col}`)
		.join(', ');

	const sql = `SELECT ${cols} FROM '${src.parquet}' WHERE total_personas > 0`;
	const result = await query(sql);
	const row = result.get(0)!.toJSON() as Record<string, any>;

	const avg = PETAL_VARS.map(v => present.has(v.col) ? (Number(row[`avg_${v.col}`]) || 1) : 1);
	_cachedProvAvg.set(territory, avg);
	return avg;
}

type RadioVars = typeof PETAL_VARS;
type RadioPop = { data: Map<string, Record<string, any>>; vars: RadioVars };

// Per-territory census source. Corrientes lacks pct_agua_red; rest identical.
const CENSUS_SRC: Record<string, { parquet: string; vars: RadioVars }> = {
	misiones:   { parquet: PARQUETS.radio_stats_master,     vars: PETAL_VARS },
	corrientes: { parquet: PARQUETS.radio_stats_corrientes, vars: PETAL_VARS.filter(v => v.col !== 'pct_agua_red') },
	// Chaco/Formosa INDEC radio_stats lack pct_agua_red (same as Corrientes).
	chaco:      { parquet: PARQUETS.radio_stats_chaco,      vars: PETAL_VARS.filter(v => v.col !== 'pct_agua_red') },
	formosa:    { parquet: PARQUETS.radio_stats_formosa,    vars: PETAL_VARS.filter(v => v.col !== 'pct_agua_red') },
	// Brazil (IBGE Censo 2022 setor-level, mode=local)
	parana_br:         { parquet: PARQUETS.radio_stats_parana_br,         vars: PETAL_VARS_BR },
	santa_catarina_br: { parquet: PARQUETS.radio_stats_santa_catarina_br, vars: PETAL_VARS_BR },
	rio_grande_sul_br: { parquet: PARQUETS.radio_stats_rio_grande_sul_br, vars: PETAL_VARS_BR },
};

const _radioPopCache = new Map<string, RadioPop>();

/**
 * Full radio-level census population for a territory, keyed by redcode, plus
 * the variable set actually available there (6 for Misiones, 5 for Corrientes).
 * Cached module-level per territory (mirrors getProvincialAvg). Read-only:
 * the returned Map is never mutated by callers.
 */
export async function loadRadioPopulation(territory: string): Promise<RadioPop> {
	const cached = _radioPopCache.get(territory);
	if (cached) return cached;

	const src = CENSUS_SRC[territory];
	if (!src) throw new Error(`No census source for territory: ${territory}`);
	if (!isReady()) throw new Error('DuckDB not ready');

	const cols = src.vars.map(v => v.col).join(', ');
	const sql = `SELECT redcode, ${cols} FROM '${src.parquet}' WHERE total_personas > 0`;
	const result = await query(sql);

	const m = new Map<string, Record<string, any>>();
	for (let i = 0; i < result.numRows; i++) {
		const row = result.get(i)!.toJSON() as Record<string, any>;
		for (const v of src.vars) row[v.col] = Number(row[v.col]);
		m.set(String(row.redcode), row);
	}

	const out: RadioPop = { data: m, vars: src.vars };
	_radioPopCache.set(territory, out);
	return out;
}
