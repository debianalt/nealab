// Hexagon fact sheet: every layer's values for one H3 cell, read from the SAME
// per-department parquets each layer panel reads (so the numbers match), plus the
// cell's position within its department (percentile + mean, computed here from
// the whole department file). Files are fetched in parallel and queried locally:
// one serial HTTP query per layer took ~18 s for 14 layers; this is ~1-2 s.
import { ANALYSIS_REGISTRY, HEX_LAYER_REGISTRY, getSatGlobalUrl, type HexVariable, type LensId } from '$lib/config';
import { query, registerBuffer } from '$lib/stores/duckdb';
import { layerDeptUrl } from '$lib/utils/layerDeptUrl';
import { unitLabel } from '$lib/utils/units';
import type { FichaContext } from '$lib/stores/ficha.svelte';
import { quantile } from '$lib/utils/chartType';

// Where a value sits within one comparison area. Today only the department (or
// the whole territory for layers not split by department) is filled, from the
// file the layer panel reads; province and region will arrive with this same
// shape from the Hexoma or a precomputed JSON.
export interface FichaScope {
	id: 'dept' | 'territory' | 'prov' | 'region';
	n: number;
	p10: number; p25: number; p50: number; p75: number; p90: number;
	mean: number;
	min: number; max: number;
	// Kept apart because the mid-rank percentile hides ties: with 60 % zeros a
	// zero cell is P30, yet it is not "higher than 30 in 100" — it equals 60.
	below: number;   // % of cells with a strictly lower value
	equal: number;   // % of cells with the same value
}

export interface FichaVar {
	labelKey: string;
	value: number | null;      // display value (raw when the layer has one, × scale)
	text: string | null;       // categorical value (type_label-like), if any
	unit: string;              // display unit ('' when none)
	pctlDept: number | null;   // 0-100: share of the department's cells below this one
	meanDept: number | null;   // department mean, same units as value
	scopes: FichaScope[];      // position of the value in each comparison area (display units)
	nodata: boolean;
}

export interface FichaLayer {
	id: string;
	titleKey: string;
	lensId: LensId;
	census: boolean;           // dasymetric census layer: only cells with buildings carry data
	scope: 'dept' | 'territory'; // what "department" context actually covers for this layer
	found: boolean;            // the cell is in this layer's file
	typeLabel: string | null;
	primary: FichaVar | null;
	vars: FichaVar[];
	error?: boolean;
}

// Same exclusions as the hex panel (HexComparison): composite/categorical helpers.
const HIDDEN = new Set(['score', 'type', 'type_label', 'pca_1', 'pca_2']);

const registered = new Map<string, string>(); // url → virtual file name
let seq = 0;

async function fetchAndRegister(url: string): Promise<string | null> {
	const hit = registered.get(url);
	if (hit) return hit;
	const res = await fetch(url);
	if (!res.ok) return null;
	const bytes = new Uint8Array(await res.arrayBuffer());
	const name = `ficha_${++seq}.parquet`;
	await registerBuffer(name, bytes);
	registered.set(url, name);
	return name;
}

// Share of cells strictly below v and equal to v, in %.
function rankOf(sortedAsc: number[], v: number): { below: number; equal: number } {
	let lo = 0, hi = sortedAsc.length;
	while (lo < hi) { const m = (lo + hi) >> 1; if (sortedAsc[m] < v) lo = m + 1; else hi = m; }
	let eqEnd = lo;
	while (eqEnd < sortedAsc.length && sortedAsc[eqEnd] === v) eqEnd++;
	const n = sortedAsc.length;
	return { below: (100 * lo) / n, equal: (100 * (eqEnd - lo)) / n };
}

// Distribution summary of one area, in display units (× scale).
function scopeOf(id: FichaScope['id'], sortedAsc: number[], v: number | null, scale: number): FichaScope {
	const q = (p: number) => quantile(sortedAsc, p) * scale;
	const mean = sortedAsc.reduce((a, x) => a + x, 0) / sortedAsc.length;
	const r = v === null ? { below: NaN, equal: NaN } : rankOf(sortedAsc, v);
	return {
		id, n: sortedAsc.length,
		p10: q(0.1), p25: q(0.25), p50: q(0.5), p75: q(0.75), p90: q(0.9),
		mean: mean * scale, min: sortedAsc[0] * scale, max: sortedAsc[sortedAsc.length - 1] * scale,
		below: r.below, equal: r.equal,
	};
}

// The column a variable displays (raw physical value when declared) and its scale/unit.
function displaySpec(v: HexVariable): { col: string; alt: string | null; scale: number; unit: string } {
	return {
		col: v.rawCol ?? v.col,
		alt: v.rawCol ? v.col : null,
		scale: v.scale ?? 1,
		unit: v.unit ? unitLabel(v.unit) : '/100',
	};
}

export async function loadFicha(
	h3: string,
	ctx: FichaContext,
	deptUrl: (layerId: string, key: string) => string | null,
): Promise<FichaLayer[]> {
	const analyses = ANALYSIS_REGISTRY.filter(a => HEX_LAYER_REGISTRY[a.id]
		&& (a.coverage as Record<string, string> | undefined)?.[ctx.territoryId] !== 'unavailable');

	// Resolve and download every layer's file in parallel.
	const prepared = await Promise.all(analyses.map(async (a) => {
		const layer = HEX_LAYER_REGISTRY[a.id];
		try {
			const perDept = !!layer.perDepartment && !!ctx.deptName;
			// A link-index key names the file that holds the cell; layers where the cell is in
			// no file (census cells without buildings) may lack a file under that key → fall
			// back to the department-name route, which then reports "sin dato" as usual.
			const key = perDept ? ctx.keys?.[a.id] : undefined;
			const keyUrl = key ? deptUrl(a.id, key) : null;
			let name = keyUrl ? await fetchAndRegister(keyUrl) : null;
			if (!name) {
				const url = perDept
					? await layerDeptUrl(a.id, ctx.territoryPrefix, ctx.deptName, ctx.deptKey, deptUrl)
					: getSatGlobalUrl(a.id, ctx.territoryPrefix);
				name = await fetchAndRegister(url);
			}
			return { a, layer, name, scope: (perDept ? 'dept' : 'territory') as 'dept' | 'territory' };
		} catch {
			return { a, layer, name: null, scope: 'territory' as const };
		}
	}));

	const out: FichaLayer[] = [];
	// Local queries: DuckDB-WASM runs them one at a time, but they no longer hit the network.
	for (const { a, layer, name, scope } of prepared) {
		const base: FichaLayer = {
			id: a.id, titleKey: a.titleKey, lensId: a.lensId, census: a.rigorBadge === 'census', scope,
			found: false, typeLabel: null, primary: null, vars: [],
		};
		if (!name) { out.push({ ...base, error: true }); continue; }
		try {
			const vars = layer.variables.filter(v => !HIDDEN.has(v.col));
			// The summary row shows the layer's primary even when the table hides it
			// (forestry's primary is its /100 score, not the first covariate).
			const pv = layer.primaryVariable;
			const primaryVar = layer.variables.find(v => v.col === pv || v.rawCol === pv);
			const primaryHidden = !!primaryVar && HIDDEN.has(primaryVar.col);
			const tbl = await query(`SELECT * FROM read_parquet('${name}')`);
			const cols = new Set(tbl.schema.fields.map((f: any) => f.name as string));
			const h3Vec = tbl.getChild('h3index');
			let row = -1;
			for (let i = 0; i < tbl.numRows; i++) if (String(h3Vec?.get(i)) === h3) { row = i; break; }
			const typeVec = cols.has('type_label') ? tbl.getChild('type_label') : null;
			const fv: FichaVar[] = [];
			for (const v of primaryHidden ? [...vars, primaryVar!] : vars) {
				const spec = displaySpec(v);
				const colName = cols.has(spec.col) ? spec.col : spec.alt && cols.has(spec.alt) ? spec.alt : null;
				if (!colName) continue;
				const vec = tbl.getChild(colName);
				const all: number[] = [];
				for (let i = 0; i < tbl.numRows; i++) {
					const x = Number(vec?.get(i));
					if (Number.isFinite(x)) all.push(x);
				}
				const raw = row >= 0 ? vec?.get(row) : null;
				const isText = typeof raw === 'string';
				const num = !isText && raw !== null && raw !== undefined ? Number(raw) : NaN;
				const has = isText ? (raw as string).length > 0 : Number.isFinite(num);
				all.sort((x, y) => x - y);
				const mean = all.length ? all.reduce((s, x) => s + x, 0) / all.length : NaN;
				const scopes = all.length && !isText ? [scopeOf(scope, all, has ? num : null, spec.scale)] : [];
				fv.push({
					labelKey: v.labelKey,
					value: has && !isText ? num * spec.scale : null,
					text: has && isText ? (raw as string) : null,
					unit: spec.unit,
					pctlDept: has && !isText && scopes.length ? scopes[0].below + scopes[0].equal / 2 : null,
					meanDept: Number.isFinite(mean) ? mean * spec.scale : null,
					scopes,
					nodata: !has,
				});
			}
			// Only pop when the appended primary actually made it (its column may be missing).
			const primary = primaryHidden
				? (fv.length && fv[fv.length - 1].labelKey === primaryVar!.labelKey ? fv.pop()! : null)
				: fv.find(x => primaryVar && x.labelKey === primaryVar.labelKey) ?? fv[0] ?? null;
			out.push({
				...base,
				found: row >= 0,
				typeLabel: row >= 0 && typeVec ? (typeVec.get(row) as string | null) ?? null : null,
				primary,
				vars: fv,
			});
		} catch {
			out.push({ ...base, error: true });
		}
	}
	return out;
}
