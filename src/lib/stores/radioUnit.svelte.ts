// The census radio as the unit of the census rates (fase 2 «soporte censal», 4-nov-2026).
//
// A census rate is measured over an INDEC radio; the map shows it on the res-9 cells with
// buildings inside the radio, so 98 % of those cells repeat the radio's value. Counting cells
// would weigh each radio as many times as it has cells: in Misiones the Moran of NBI reads
// I 0,835 / z 243 over cells and I 0,197 / z 13,6 over radios. The charts use this unit for the
// variables listed in RADIO_SUPPORT; everything else (satellite layers, densities, VIIRS) keeps
// the cell. Data: pipeline/build_census_radios.py → {prefix}census_radios_2022.parquet.
import { query } from '$lib/stores/duckdb';
import { getCensusRadiosUrl } from '$lib/config';

// Layer → columns whose support is the radio (same rule as metodologia.SOPORTES in the Hexoma).
export const RADIO_SUPPORT: Record<string, ReadonlySet<string>> = {
	service_deprivation: new Set(['c_nbi', 'c_sin_cloacas', 'c_piso', 'c_hacinamiento', 'c_hacinamiento_crit', 'c_sin_computadora']),
	health_access: new Set(['c_healthcare_time', 'c_health_coverage', 'c_nbi', 'c_elderly', 'c_children']),
	education_capital: new Set(['c_no_schooling', 'c_secondary_plus', 'c_higher_edu', 'c_university']),
	education_flow: new Set(['c_dropout_primary', 'c_dropout_secondary']),
	sociodemographic: new Set(['pct_nbi', 'pct_hacinamiento', 'pct_propietario', 'pct_computadora', 'tamano_medio_hogar']),
	economic_activity: new Set(['tasa_empleo', 'tasa_actividad', 'pct_universitario']),
};

// Territories with INDEC radios: parquet prefix → INDEC province code (codprov of the radio tiles).
const RADIO_TERRITORIES: Record<string, string> = { '': '54', 'corrientes/': '18', 'chaco/': '22', 'formosa/': '34' };

export function isRadioSupport(layerId: string | null | undefined, col: string | null | undefined): boolean {
	return !!layerId && !!col && !!RADIO_SUPPORT[layerId]?.has(col);
}

export function hasRadios(territoryPrefix: string): boolean {
	return territoryPrefix in RADIO_TERRITORIES;
}

/** Click selection by radio: census layers in a territory with radios, cell view (not aggregated). */
export function radioSelectionOn(layerId: string | null | undefined, territoryPrefix: string, aggregated: boolean): boolean {
	return !!layerId && layerId in RADIO_SUPPORT && hasRadios(territoryPrefix) && !aggregated;
}

/** INDEC redcode as read on the census cartography: 540070303 → 54-007-03-03. */
export function fmtRedcode(id: string): string {
	return /^\d{9}$/.test(id) ? `${id.slice(0, 2)}-${id.slice(2, 5)}-${id.slice(5, 7)}-${id.slice(7)}` : id;
}

export interface SelectedRadio {
	redcode: string;
	/** The radio's row of census_radios_2022 (rates and bases of the whole radio). */
	row: Record<string, any>;
	/** Selected cells inside the radio, in selection order. */
	hexes: string[];
	/** Colour of the radio's first selected cell; every cell of the radio takes it. */
	color: string;
}

export function codprovOf(territoryPrefix: string): string | null {
	return RADIO_TERRITORIES[territoryPrefix] ?? null;
}

export interface RadioView {
	/** redcode → radio row (rates, bases, dpto_key, n_celdas). */
	data: Map<string, Record<string, any>>;
	/** Queen neighbours restricted to the radios in `data`. */
	neighbors: (id: string) => string[];
	/** Radio → its cells present in the cell data the view was built from. */
	cellsOf: (id: string) => string[];
	/** Every cell of the given radios (for a brush or a zone). */
	expand: (ids: Iterable<string>) => string[];
}

interface TerritoryRadios {
	rows: Map<string, Record<string, any>>;
	vecinos: Map<string, string[]>;
	cellToRadio: Map<string, string>;
}

class RadioUnitStore {
	/** Loaded territories by parquet prefix; reactive so views recompute when a load lands. */
	loaded: Map<string, TerritoryRadios> = $state.raw(new Map());
	private pending = new Map<string, Promise<void>>();
	error: string | null = $state(null);

	ensure(territoryPrefix: string): Promise<void> {
		if (!hasRadios(territoryPrefix)) return Promise.resolve();
		if (this.loaded.has(territoryPrefix)) return Promise.resolve();
		const p = this.pending.get(territoryPrefix);
		if (p) return p;
		const job = this.load(territoryPrefix).finally(() => this.pending.delete(territoryPrefix));
		this.pending.set(territoryPrefix, job);
		return job;
	}

	private async load(prefix: string): Promise<void> {
		try {
			const res = await query(`SELECT * FROM '${getCensusRadiosUrl(prefix)}'`);
			const rows = new Map<string, Record<string, any>>();
			const vecinos = new Map<string, string[]>();
			const cellToRadio = new Map<string, string>();
			const fields = res.schema.fields.map(f => f.name).filter(n => n !== 'vecinos' && n !== 'celdas');
			const cols = Object.fromEntries(fields.map(n => [n, res.getChild(n)]));
			const vcol = res.getChild('vecinos');
			const ccol = res.getChild('celdas');
			for (let i = 0; i < res.numRows; i++) {
				const row: Record<string, any> = {};
				for (const n of fields) {
					const v = cols[n]?.get(i);
					row[n] = typeof v === 'bigint' ? Number(v) : v;
				}
				const id = String(row.redcode);
				rows.set(id, row);
				vecinos.set(id, listOf(vcol?.get(i)));
				for (const h of listOf(ccol?.get(i))) cellToRadio.set(h, id);
			}
			const next = new Map(this.loaded);
			next.set(prefix, { rows, vecinos, cellToRadio });
			this.loaded = next;
			this.error = null;
		} catch (e) {
			this.error = e instanceof Error ? e.message : String(e);
			console.warn('[radioUnit] no se pudieron cargar los radios', prefix, e);
		}
	}

	radioOf(territoryPrefix: string, h3: string): Record<string, any> | null {
		const t = this.loaded.get(territoryPrefix);
		const id = t?.cellToRadio.get(h3);
		return id ? t!.rows.get(id) ?? null : null;
	}

	/**
	 * The click selection grouped by radio, in selection order: a second cell of a radio
	 * already picked joins it instead of repeating its rates. Cells with no radio stay in
	 * `loose`. Null when the territory's radios are not loaded (callers fall back to cells).
	 */
	groupSelection(territoryPrefix: string, selected: Map<string, { color: string }>): { radios: SelectedRadio[]; loose: string[] } | null {
		const t = this.loaded.get(territoryPrefix);
		if (!t || selected.size === 0) return null;
		const by = new Map<string, SelectedRadio>();
		const loose: string[] = [];
		for (const [h, s] of selected) {
			const id = t.cellToRadio.get(h);
			const row = id ? t.rows.get(id) : undefined;
			if (!id || !row) { loose.push(h); continue; }
			let g = by.get(id);
			if (!g) by.set(id, g = { redcode: id, row, hexes: [], color: s.color });
			g.hexes.push(h);
		}
		return { radios: [...by.values()], loose };
	}

	/**
	 * The radios behind a set of cells (the open department, a zone, a filter): every radio
	 * with at least one of those cells. Null when the territory has no radios or is not loaded,
	 * so callers fall back to the cells.
	 */
	view(territoryPrefix: string, cells: Map<string, unknown>): RadioView | null {
		const t = this.loaded.get(territoryPrefix);
		if (!t || cells.size === 0) return null;
		const cellsBy = new Map<string, string[]>();
		for (const h of cells.keys()) {
			const id = t.cellToRadio.get(h);
			if (!id) continue;
			let a = cellsBy.get(id);
			if (!a) cellsBy.set(id, a = []);
			a.push(h);
		}
		if (cellsBy.size === 0) return null;
		const data = new Map<string, Record<string, any>>();
		for (const id of cellsBy.keys()) { const r = t.rows.get(id); if (r) data.set(id, r); }
		return {
			data,
			neighbors: (id) => (t.vecinos.get(id) ?? []).filter(n => data.has(n)),
			cellsOf: (id) => cellsBy.get(id) ?? [],
			expand: (ids) => { const out: string[] = []; for (const id of ids) out.push(...(cellsBy.get(id) ?? [])); return out; },
		};
	}
}

function listOf(v: any): string[] {
	if (!v) return [];
	if (Array.isArray(v)) return v.map(String);
	if (typeof v[Symbol.iterator] === 'function') return Array.from(v as Iterable<any>, x => String(x));   // Arrow list → Vector
	return [];
}

export const radioUnit = new RadioUnitStore();
