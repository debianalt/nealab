import { getBrDistrictsUrl } from '$lib/config';

// AR census dept polygons (~465KB) are only needed for AR perDepartment/census
// interactions, never on cold open. Lazy-loaded (mirrors ensureBrBoundaries) so they
// stay out of the initial bundle; findDeptFeature returns null until the import lands.
//
// Two AR polygon sets, because two partitions coexist in the data (measured 2-nov-2026,
// pipeline/build_ar_dept_boundaries_indec.py): Misiones and every census layer were split
// by INDEC census radios; the satellite layers of Corrientes, Chaco and Formosa by GADM
// departments. Line, clip and hole-fill must use the polygon of the layer's partition
// (arBoundarySource), or the hexagons spill over or stop short of the line. INDEC =
// radios dissolved by department (ar_dept_boundaries_indec.json); GADM = the old file.
// When those satellite layers are re-split by INDEC radios, the GADM file goes.
export type ArBoundarySource = 'indec' | 'gadm';
let arFeatures: any[] | null = null;
let arIndecFeatures: any[] | null = null;
let arPending: Promise<unknown> | null = null;

export async function ensureArBoundaries(): Promise<void> {
	if (arFeatures && arIndecFeatures) return;
	if (!arPending) {
		arPending = Promise.all([
			import('$lib/data/ar_dept_boundaries.json').then((m) => (arFeatures = (m.default as any).features as any[])),
			import('$lib/data/ar_dept_boundaries_indec.json').then((m) => (arIndecFeatures = (m.default as any).features as any[])),
		]);
		arPending.catch(() => { arPending = null; }); // retry on transient failure
	}
	try { await arPending; } catch { /* swallow: caller tolerates null → outline absent */ }
}

/** The AR polygons that match a layer's department partition. */
export function arBoundarySource(territoryPrefix: string, census: boolean): ArBoundarySource {
	return territoryPrefix === '' || census ? 'indec' : 'gadm';
}

export function getArFeatures(source: ArBoundarySource = 'gadm'): any[] | null {
	return source === 'indec' ? arIndecFeatures : arFeatures;
}

// Department names differ in case, accents and numerals between the polygons
// (INDEC, DGEEC, IBGE) and the summaries: compare them through this key.
// Chaco's summaries spell out four numbered departments ("Doce de Octubre")
// that the INDEC polygons write with digits ("12 de Octubre"); Misiones'
// summaries abbreviate three ("G. M. Belgrano" for General Manuel Belgrano).
const DEPT_ALIASES: Record<string, string> = {
	docedeoctubre: '12deoctubre', nuevedejulio: '9dejulio',
	primerodemayo: '1demayo', veinticincodemayo: '25demayo',
	gmbelgrano: 'generalmanuelbelgrano', lnalem: 'leandronalem', lgsanmartin: 'libertadorgeneralsanmartin',
};
export function normDeptName(s: string | null | undefined): string {
	const k = (s ?? '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9]/g, '');
	return DEPT_ALIASES[k] ?? k;
}

// territoryPrefix → INDEC codprov (2-digit). Used to disambiguate dept names
// across provinces (e.g., "General Belgrano" exists in multiple provinces).
const AR_PROVINCE_PREFIX: Record<string, string> = {
	'': '54',            // Misiones (default territory)
	'corrientes/': '18',
	'chaco/': '22',
	'formosa/': '34',
};

// True for the 4 AR territories that have dept polygons in ar_dept_boundaries.json.
// Callers use it to gate ensureArBoundaries() so PY/BR views don't fetch the 465KB.
export function isArDeptTerritory(territoryPrefix: string): boolean {
	return AR_PROVINCE_PREFIX[territoryPrefix] !== undefined;
}

// BR municipality boundaries are fetched on demand (Option B) — too large to
// bundle (1193 municipios across PR+SC+RS, ~3.7 MB total). Module-level cache:
// once a territory is loaded, subsequent lookups are sync.
const brBoundariesCache = new Map<string, any>();
const brBoundariesPending = new Map<string, Promise<any>>();

// BR municipalities and PY districts share the cache. PY: the same polygons the district
// tiles and the cell crosswalk were built from (pipeline/build_itapua_districts.py
// --export-geojson → static/data/<stem>_districts.geojson, INE where it exists, else GAUL).
function brTerritoryId(territoryPrefix: string): string | null {
	return territoryPrefix.endsWith('_br/') || territoryPrefix.endsWith('_py/') ? territoryPrefix.slice(0, -1) : null;
}
const districtsUrl = (territoryId: string) =>
	territoryId.endsWith('_py') ? `/data/${territoryId.slice(0, -3)}_districts.geojson` : getBrDistrictsUrl(territoryId);

export async function ensureBrBoundaries(territoryId: string): Promise<void> {
	if (!territoryId.endsWith('_br') && !territoryId.endsWith('_py')) return;
	if (brBoundariesCache.has(territoryId)) return;
	const pending = brBoundariesPending.get(territoryId);
	if (pending) { await pending; return; }
	const promise = fetch(districtsUrl(territoryId))
		.then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
		.then(fc => { brBoundariesCache.set(territoryId, fc); brBoundariesPending.delete(territoryId); return fc; })
		.catch(e => { brBoundariesPending.delete(territoryId); console.warn('[ensureBrBoundaries]', territoryId, e); throw e; });
	brBoundariesPending.set(territoryId, promise);
	try { await promise; } catch { /* swallow: findDeptFeature returns null → polygon outline silently absent */ }
}

export function getBrFeatures(territoryId: string): any[] | null {
	return brBoundariesCache.get(territoryId)?.features ?? null;
}

export function findDeptFeature(deptName: string, territoryPrefix: string, census = false): any | null {
	if (!deptName) return null;

	// AR path: bundled GeoJSON, sync lookup by codprov prefix + nombre.
	const provincePrefix = AR_PROVINCE_PREFIX[territoryPrefix];
	if (provincePrefix) {
		// null until ensureArBoundaries() resolves — callers (loadDept hole-fill,
		// outline render) already tolerate null, same as the BR branch below.
		const key = normDeptName(deptName);
		return (getArFeatures(arBoundarySource(territoryPrefix, census)) ?? []).find(f =>
			String(f.properties.redcode).startsWith(provincePrefix) &&
			normDeptName(f.properties.nombre) === key
		) || null;
	}

	// BR path: cache lookup. If ensureBrBoundaries hasn't resolved yet, return
	// null — callers (loadDept hole-fill, polygon outline render) already tolerate
	// null. The cache fills as soon as the pre-fetch promise resolves; the next
	// call returns the feature.
	const territoryId = brTerritoryId(territoryPrefix);
	if (territoryId) {
		const fc = brBoundariesCache.get(territoryId);
		if (!fc) return null;
		if (territoryId.endsWith('_py')) {
			const key = normDeptName(deptName);   // summaries and polygons differ in case and accents
			return fc.features.find((f: any) => normDeptName(f.properties?.district) === key) ?? null;
		}
		return fc.features.find((f: any) => f.properties?.district === deptName) ?? null;
	}

	return null;
}
