import { getBrDistrictsUrl } from '$lib/config';

// AR census dept polygons (~465KB) are only needed for AR perDepartment/census
// interactions, never on cold open. Lazy-loaded (mirrors ensureBrBoundaries) so they
// stay out of the initial bundle; findDeptFeature returns null until the import lands.
let arFeatures: any[] | null = null;
let arPending: Promise<any[]> | null = null;

export async function ensureArBoundaries(): Promise<void> {
	if (arFeatures) return;
	if (!arPending) {
		arPending = import('$lib/data/ar_dept_boundaries.json')
			.then((m) => (arFeatures = (m.default as any).features as any[]));
		arPending.catch(() => { arPending = null; }); // retry on transient failure
	}
	try { await arPending; } catch { /* swallow: caller tolerates null → outline absent */ }
}

export function getArFeatures(): any[] | null {
	return arFeatures;
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

function brTerritoryId(territoryPrefix: string): string | null {
	return territoryPrefix.endsWith('_br/') ? territoryPrefix.slice(0, -1) : null;
}

export async function ensureBrBoundaries(territoryId: string): Promise<void> {
	if (!territoryId.endsWith('_br')) return;
	if (brBoundariesCache.has(territoryId)) return;
	const pending = brBoundariesPending.get(territoryId);
	if (pending) { await pending; return; }
	const promise = fetch(getBrDistrictsUrl(territoryId))
		.then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
		.then(fc => { brBoundariesCache.set(territoryId, fc); brBoundariesPending.delete(territoryId); return fc; })
		.catch(e => { brBoundariesPending.delete(territoryId); console.warn('[ensureBrBoundaries]', territoryId, e); throw e; });
	brBoundariesPending.set(territoryId, promise);
	try { await promise; } catch { /* swallow: findDeptFeature returns null → polygon outline silently absent */ }
}

export function findDeptFeature(deptName: string, territoryPrefix: string): any | null {
	if (!deptName) return null;

	// AR path: bundled GeoJSON, sync lookup by codprov prefix + nombre.
	const provincePrefix = AR_PROVINCE_PREFIX[territoryPrefix];
	if (provincePrefix) {
		// null until ensureArBoundaries() resolves — callers (loadDept hole-fill,
		// outline render) already tolerate null, same as the BR branch below.
		return (arFeatures ?? []).find(f =>
			f.properties.nombre === deptName &&
			String(f.properties.redcode).startsWith(provincePrefix)
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
		return fc.features.find((f: any) => f.properties?.district === deptName) ?? null;
	}

	return null;
}
