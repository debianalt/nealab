// Shared dept summary loaders — used by OvertureAnalysis and ComparisonPanel.
// Each loader returns the bundled JSON for a given analysis × territory.

const SAT_SUMMARIES: Record<string, () => Promise<any>> = {
	agri_potential:         () => import('$lib/data/sat_agri_apt_dept_summary.json'),
	forestry_aptitude:      () => import('$lib/data/sat_forestry_aptitude_dept_summary.json'),
	service_deprivation:    () => import('$lib/data/sat_service_deprivation_dept_summary.json'),
	health_access:          () => import('$lib/data/sat_health_access_dept_summary.json'),
	education_capital:      () => import('$lib/data/sat_education_capital_dept_summary.json'),
	education_flow:         () => import('$lib/data/sat_education_flow_dept_summary.json'),
	land_use:               () => import('$lib/data/sat_land_use_dept_summary.json'),
	flood_risk:             () => import('$lib/data/flood_dept_summary.json'),
	sociodemographic:       () => import('$lib/data/sat_sociodemographic_dept_summary.json'),
	economic_activity:      () => import('$lib/data/sat_economic_activity_dept_summary.json'),
	accessibility:          () => import('$lib/data/sat_accessibility_dept_summary.json'),
	pm25_drivers:           () => import('$lib/data/sat_pm25_drivers_dept_summary.json'),
	deforestation_dynamics: () => import('$lib/data/sat_deforestation_dynamics_dept_summary.json'),
};

const ITAPUA_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics: () => import('$lib/data/itapua_py_sat_deforestation_dynamics_summary.json'),
	pm25_drivers:           () => import('$lib/data/itapua_py_sat_pm25_drivers_summary.json'),
	forestry_aptitude:      () => import('$lib/data/itapua_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/itapua_py_sat_land_use_summary.json'),
	accessibility:          () => import('$lib/data/itapua_py_sat_accessibility_summary.json'),
	flood_risk:             () => import('$lib/data/itapua_py_flood_dept_summary.json'),
};

const CORRIENTES_SUMMARIES: Record<string, () => Promise<any>> = {
	agri_potential:         () => import('$lib/data/corrientes_sat_agri_apt_summary.json'),
	accessibility:          () => import('$lib/data/corrientes_sat_accessibility_summary.json'),
	pm25_drivers:           () => import('$lib/data/corrientes_sat_pm25_drivers_summary.json'),
	land_use:               () => import('$lib/data/corrientes_sat_land_use_summary.json'),
	sociodemographic:       () => import('$lib/data/corrientes_sat_sociodemographic_summary.json'),
	economic_activity:      () => import('$lib/data/corrientes_sat_economic_activity_summary.json'),
	flood_risk:             () => import('$lib/data/corrientes_flood_dept_summary.json'),
	service_deprivation:    () => import('$lib/data/corrientes_sat_service_deprivation_summary.json'),
	health_access:          () => import('$lib/data/corrientes_sat_health_access_summary.json'),
	education_capital:      () => import('$lib/data/corrientes_sat_education_capital_summary.json'),
	education_flow:         () => import('$lib/data/corrientes_sat_education_flow_summary.json'),
	deforestation_dynamics: () => import('$lib/data/corrientes_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/corrientes_sat_forestry_aptitude_summary.json'),
};

// Alto Paraná (PY) — Phase 1: 6 core satellite analyses. More added per phase.
const ALTO_PARANA_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics: () => import('$lib/data/alto_parana_py_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/alto_parana_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/alto_parana_py_sat_land_use_summary.json'),
	pm25_drivers:           () => import('$lib/data/alto_parana_py_sat_pm25_drivers_summary.json'),
	flood_risk:             () => import('$lib/data/alto_parana_py_flood_dept_summary.json'),
	accessibility:          () => import('$lib/data/alto_parana_py_sat_accessibility_summary.json'),
};

const CHACO_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics  : () => import('$lib/data/chaco_sat_deforestation_dynamics_summary.json'),
	flood_risk              : () => import('$lib/data/chaco_flood_dept_summary.json'),
	pm25_drivers            : () => import('$lib/data/chaco_sat_pm25_drivers_summary.json'),
	forestry_aptitude       : () => import('$lib/data/chaco_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/chaco_sat_agri_apt_summary.json'),
	land_use                : () => import('$lib/data/chaco_sat_land_use_summary.json'),
	accessibility           : () => import('$lib/data/chaco_sat_accessibility_summary.json'),
	economic_activity       : () => import('$lib/data/chaco_sat_economic_activity_summary.json'),
	sociodemographic        : () => import('$lib/data/chaco_sat_sociodemographic_summary.json'),
	service_deprivation     : () => import('$lib/data/chaco_sat_service_deprivation_summary.json'),
	health_access           : () => import('$lib/data/chaco_sat_health_access_summary.json'),
	education_capital       : () => import('$lib/data/chaco_sat_education_capital_summary.json'),
	education_flow          : () => import('$lib/data/chaco_sat_education_flow_summary.json'),
};
const FORMOSA_SUMMARIES: Record<string, () => Promise<any>> = {
	pm25_drivers            : () => import('$lib/data/formosa_sat_pm25_drivers_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/formosa_sat_deforestation_dynamics_summary.json'),
	flood_risk              : () => import('$lib/data/formosa_flood_dept_summary.json'),
	forestry_aptitude       : () => import('$lib/data/formosa_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/formosa_sat_agri_apt_summary.json'),
	land_use                : () => import('$lib/data/formosa_sat_land_use_summary.json'),
	accessibility           : () => import('$lib/data/formosa_sat_accessibility_summary.json'),
	economic_activity       : () => import('$lib/data/formosa_sat_economic_activity_summary.json'),
	sociodemographic        : () => import('$lib/data/formosa_sat_sociodemographic_summary.json'),
	service_deprivation     : () => import('$lib/data/formosa_sat_service_deprivation_summary.json'),
	health_access           : () => import('$lib/data/formosa_sat_health_access_summary.json'),
	education_capital       : () => import('$lib/data/formosa_sat_education_capital_summary.json'),
	education_flow          : () => import('$lib/data/formosa_sat_education_flow_summary.json'),
};

const TERRITORY_SUMMARIES: Record<string, Record<string, () => Promise<any>>> = {
	'itapua_py/': ITAPUA_SUMMARIES,
	'corrientes/': CORRIENTES_SUMMARIES,
	'alto_parana_py/': ALTO_PARANA_SUMMARIES,
	'chaco/': CHACO_SUMMARIES,
	'formosa/': FORMOSA_SUMMARIES,
	// Analysis layers only where we work (3-nov-2026): NEA AR + Itapúa + Alto Paraná. The
	// other 15 Paraguayan departments and the 3 Brazilian states keep buildings, census
	// (IBGE setores in Brazil) and limits — see config.ts coverage.
};

// EUDR (pipeline/build_eudr_dpto.py): sat_eudr_dept_summary.json (Misiones) and
// {territory}_sat_eudr_summary.json; no file for a territory → no layer there.
const EUDR_SUMMARIES = import.meta.glob('$lib/data/*sat_eudr*summary.json');

export async function loadDeptSummary(analysisId: string, territoryPrefix: string): Promise<any> {
	if (analysisId === 'eudr') {
		const name = territoryPrefix ? `${territoryPrefix.slice(0, -1)}_sat_eudr_summary` : 'sat_eudr_dept_summary';
		const loader = Object.entries(EUDR_SUMMARIES).find(([p]) => p.endsWith(`/${name}.json`))?.[1];
		if (!loader) return null;
		try {
			const mod: any = await loader();
			return mod.default ?? mod;
		} catch {
			return null;
		}
	}
	const summaries = territoryPrefix ? (TERRITORY_SUMMARIES[territoryPrefix] ?? null) : SAT_SUMMARIES;
	if (!summaries) return null;
	const loader = summaries[analysisId];
	if (!loader) return null;
	try {
		const mod = await loader();
		return mod.default ?? mod;
	} catch {
		return null;
	}
}

export interface DeptItem {
	name: string;
	parquetKey: string;
}

export async function loadDeptList(analysisId: string, territoryPrefix: string): Promise<DeptItem[]> {
	const summary = await loadDeptSummary(analysisId, territoryPrefix);
	if (!summary?.departments) return [];
	const items = (summary.departments as any[])
		.map(d => ({ name: (d.dpto ?? d.distrito ?? d.municipio ?? '') as string, parquetKey: d.parquetKey as string }))
		.filter(d => d.name && d.parquetKey)
		.sort((a, b) => a.name.localeCompare(b.name));
	// Dedupe by parquetKey: two name variants of the same admin unit (e.g. guaira's
	// "Cnel Martinez" / "Cnel. Martinez") share a key, and a duplicate key crashes
	// the keyed {#each} of the compare dropdown (each_key_duplicate).
	const seen = new Set<string>();
	return items.filter(d => (seen.has(d.parquetKey) ? false : (seen.add(d.parquetKey), true)));
}
