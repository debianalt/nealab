import boundaries from '$lib/data/ar_dept_boundaries.json';

const features = (boundaries as any).features as any[];

// territoryPrefix → INDEC codprov (2-digit). Used to disambiguate dept names
// across provinces (e.g., "General Belgrano" exists in multiple provinces).
const AR_PROVINCE_PREFIX: Record<string, string> = {
	'': '54',            // Misiones (default territory)
	'corrientes/': '18',
	'chaco/': '22',
	'formosa/': '34',
};

export function findDeptFeature(deptName: string, territoryPrefix: string): any | null {
	if (!deptName) return null;
	const provincePrefix = AR_PROVINCE_PREFIX[territoryPrefix];
	if (!provincePrefix) return null;
	return features.find(f =>
		f.properties.nombre === deptName &&
		String(f.properties.redcode).startsWith(provincePrefix)
	) || null;
}
