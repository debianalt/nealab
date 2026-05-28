import boundaries from '$lib/data/ar_dept_boundaries.json';
import { getBrDistrictsUrl } from '$lib/config';

const features = (boundaries as any).features as any[];

// territoryPrefix → INDEC codprov (2-digit). Used to disambiguate dept names
// across provinces (e.g., "General Belgrano" exists in multiple provinces).
const AR_PROVINCE_PREFIX: Record<string, string> = {
	'': '54',            // Misiones (default territory)
	'corrientes/': '18',
	'chaco/': '22',
	'formosa/': '34',
};

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
		return features.find(f =>
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
