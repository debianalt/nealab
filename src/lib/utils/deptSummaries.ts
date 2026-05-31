// Shared dept summary loaders — used by OvertureAnalysis and ComparisonPanel.
// Each loader returns the bundled JSON for a given analysis × territory.

const SAT_SUMMARIES: Record<string, () => Promise<any>> = {
	location_value:       () => import('$lib/data/sat_location_value_dept_summary.json'),
	agri_potential:       () => import('$lib/data/sat_agri_potential_dept_summary.json'),
	forestry_aptitude:    () => import('$lib/data/sat_forestry_aptitude_dept_summary.json'),
	service_deprivation:  () => import('$lib/data/sat_service_deprivation_dept_summary.json'),
	territorial_isolation: () => import('$lib/data/sat_territorial_isolation_dept_summary.json'),
	health_access:        () => import('$lib/data/sat_health_access_dept_summary.json'),
	education_capital:    () => import('$lib/data/sat_education_capital_dept_summary.json'),
	education_flow:       () => import('$lib/data/sat_education_flow_dept_summary.json'),
	land_use:             () => import('$lib/data/sat_land_use_dept_summary.json'),
	flood_risk:           () => import('$lib/data/flood_dept_summary.json'),
	territorial_scores:   () => import('$lib/data/scores_dept_summary.json'),
	sociodemographic:     () => import('$lib/data/sat_sociodemographic_dept_summary.json'),
	economic_activity:    () => import('$lib/data/sat_economic_activity_dept_summary.json'),
	accessibility:        () => import('$lib/data/sat_accessibility_dept_summary.json'),
	carbon_stock:         () => import('$lib/data/sat_carbon_stock_dept_summary.json'),
	pm25_drivers:         () => import('$lib/data/sat_pm25_drivers_dept_summary.json'),
	deforestation_dynamics: () => import('$lib/data/sat_deforestation_dynamics_dept_summary.json'),
	soil_water:             () => import('$lib/data/sat_soil_water_dept_summary.json'),
};

const ITAPUA_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics: () => import('$lib/data/itapua_py_sat_deforestation_dynamics_summary.json'),
	agri_potential:         () => import('$lib/data/itapua_py_sat_agri_potential_summary.json'),
	carbon_stock:           () => import('$lib/data/itapua_py_sat_carbon_stock_summary.json'),
	pm25_drivers:           () => import('$lib/data/itapua_py_sat_pm25_drivers_summary.json'),
	forestry_aptitude:      () => import('$lib/data/itapua_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/itapua_py_sat_land_use_summary.json'),
	accessibility:          () => import('$lib/data/itapua_py_sat_accessibility_summary.json'),
	flood_risk:             () => import('$lib/data/itapua_py_flood_dept_summary.json'),
	territorial_scores:     () => import('$lib/data/itapua_py_scores_dept_summary.json'),
	soil_water:             () => import('$lib/data/itapua_py_sat_soil_water_summary.json'),
};

const CORRIENTES_SUMMARIES: Record<string, () => Promise<any>> = {
	agri_potential:         () => import('$lib/data/corrientes_sat_agri_potential_summary.json'),
	accessibility:          () => import('$lib/data/corrientes_sat_accessibility_summary.json'),
	carbon_stock:           () => import('$lib/data/corrientes_sat_carbon_stock_summary.json'),
	pm25_drivers:           () => import('$lib/data/corrientes_sat_pm25_drivers_summary.json'),
	land_use:               () => import('$lib/data/corrientes_sat_land_use_summary.json'),
	soil_water:             () => import('$lib/data/corrientes_sat_soil_water_summary.json'),
	sociodemographic:       () => import('$lib/data/corrientes_sat_sociodemographic_summary.json'),
	economic_activity:      () => import('$lib/data/corrientes_sat_economic_activity_summary.json'),
	flood_risk:             () => import('$lib/data/corrientes_flood_dept_summary.json'),
	territorial_scores:     () => import('$lib/data/corrientes_scores_dept_summary.json'),
	service_deprivation:    () => import('$lib/data/corrientes_sat_service_deprivation_summary.json'),
	territorial_isolation:  () => import('$lib/data/corrientes_sat_territorial_isolation_summary.json'),
	health_access:          () => import('$lib/data/corrientes_sat_health_access_summary.json'),
	education_capital:      () => import('$lib/data/corrientes_sat_education_capital_summary.json'),
	education_flow:         () => import('$lib/data/corrientes_sat_education_flow_summary.json'),
	deforestation_dynamics: () => import('$lib/data/corrientes_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/corrientes_sat_forestry_aptitude_summary.json'),
	location_value:         () => import('$lib/data/corrientes_sat_location_value_summary.json'),
};

// Alto Paraná (PY) — Phase 1: 6 core satellite analyses. More added per phase.
const ALTO_PARANA_SUMMARIES: Record<string, () => Promise<any>> = {
	agri_potential:         () => import('$lib/data/alto_parana_py_sat_agri_potential_summary.json'),
	carbon_stock:           () => import('$lib/data/alto_parana_py_sat_carbon_stock_summary.json'),
	deforestation_dynamics: () => import('$lib/data/alto_parana_py_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/alto_parana_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/alto_parana_py_sat_land_use_summary.json'),
	pm25_drivers:           () => import('$lib/data/alto_parana_py_sat_pm25_drivers_summary.json'),
	soil_water:             () => import('$lib/data/alto_parana_py_sat_soil_water_summary.json'),
	flood_risk:             () => import('$lib/data/alto_parana_py_flood_dept_summary.json'),
	territorial_scores:     () => import('$lib/data/alto_parana_py_scores_dept_summary.json'),
	accessibility:          () => import('$lib/data/alto_parana_py_sat_accessibility_summary.json'),
};

const CHACO_SUMMARIES: Record<string, () => Promise<any>> = {
	carbon_stock            : () => import('$lib/data/chaco_sat_carbon_stock_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/chaco_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/chaco_scores_dept_summary.json'),
	soil_water              : () => import('$lib/data/chaco_sat_soil_water_summary.json'),
	flood_risk              : () => import('$lib/data/chaco_flood_dept_summary.json'),
	pm25_drivers            : () => import('$lib/data/chaco_sat_pm25_drivers_summary.json'),
	forestry_aptitude       : () => import('$lib/data/chaco_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/chaco_sat_agri_potential_summary.json'),
	land_use                : () => import('$lib/data/chaco_sat_land_use_summary.json'),
	sociodemographic        : () => import('$lib/data/chaco_sat_sociodemographic_summary.json'),
	service_deprivation     : () => import('$lib/data/chaco_sat_service_deprivation_summary.json'),
	territorial_isolation   : () => import('$lib/data/chaco_sat_territorial_isolation_summary.json'),
	health_access           : () => import('$lib/data/chaco_sat_health_access_summary.json'),
	education_capital       : () => import('$lib/data/chaco_sat_education_capital_summary.json'),
	education_flow          : () => import('$lib/data/chaco_sat_education_flow_summary.json'),
	location_value          : () => import('$lib/data/chaco_sat_location_value_summary.json'),
};
const FORMOSA_SUMMARIES: Record<string, () => Promise<any>> = {
	carbon_stock            : () => import('$lib/data/formosa_sat_carbon_stock_summary.json'),
	pm25_drivers            : () => import('$lib/data/formosa_sat_pm25_drivers_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/formosa_sat_deforestation_dynamics_summary.json'),
	soil_water              : () => import('$lib/data/formosa_sat_soil_water_summary.json'),
	territorial_scores      : () => import('$lib/data/formosa_scores_dept_summary.json'),
	flood_risk              : () => import('$lib/data/formosa_flood_dept_summary.json'),
	forestry_aptitude       : () => import('$lib/data/formosa_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/formosa_sat_agri_potential_summary.json'),
	land_use                : () => import('$lib/data/formosa_sat_land_use_summary.json'),
	sociodemographic        : () => import('$lib/data/formosa_sat_sociodemographic_summary.json'),
	service_deprivation     : () => import('$lib/data/formosa_sat_service_deprivation_summary.json'),
	territorial_isolation   : () => import('$lib/data/formosa_sat_territorial_isolation_summary.json'),
	health_access           : () => import('$lib/data/formosa_sat_health_access_summary.json'),
	education_capital       : () => import('$lib/data/formosa_sat_education_capital_summary.json'),
	education_flow          : () => import('$lib/data/formosa_sat_education_flow_summary.json'),
	location_value          : () => import('$lib/data/formosa_sat_location_value_summary.json'),
};
const PARANA_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics  : () => import('$lib/data/parana_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/parana_br_scores_dept_summary.json'),
	flood_risk              : () => import('$lib/data/parana_br_flood_dept_summary.json'),
	carbon_stock            : () => import('$lib/data/parana_br_sat_carbon_stock_summary.json'),
	soil_water              : () => import('$lib/data/parana_br_sat_soil_water_summary.json'),
	pm25_drivers            : () => import('$lib/data/parana_br_sat_pm25_drivers_summary.json'),
	forestry_aptitude       : () => import('$lib/data/parana_br_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/parana_br_sat_agri_potential_summary.json'),
	land_use                : () => import('$lib/data/parana_br_sat_land_use_summary.json'),
};
const SANTA_CATARINA_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics  : () => import('$lib/data/santa_catarina_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/santa_catarina_br_scores_dept_summary.json'),
	carbon_stock            : () => import('$lib/data/santa_catarina_br_sat_carbon_stock_summary.json'),
	pm25_drivers            : () => import('$lib/data/santa_catarina_br_sat_pm25_drivers_summary.json'),
	flood_risk              : () => import('$lib/data/santa_catarina_br_flood_dept_summary.json'),
	soil_water              : () => import('$lib/data/santa_catarina_br_sat_soil_water_summary.json'),
	forestry_aptitude       : () => import('$lib/data/santa_catarina_br_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/santa_catarina_br_sat_agri_potential_summary.json'),
	land_use                : () => import('$lib/data/santa_catarina_br_sat_land_use_summary.json'),
};
const RIO_GRANDE_SUL_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	deforestation_dynamics  : () => import('$lib/data/rio_grande_sul_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/rio_grande_sul_br_scores_dept_summary.json'),
	flood_risk              : () => import('$lib/data/rio_grande_sul_br_flood_dept_summary.json'),
	carbon_stock            : () => import('$lib/data/rio_grande_sul_br_sat_carbon_stock_summary.json'),
	soil_water              : () => import('$lib/data/rio_grande_sul_br_sat_soil_water_summary.json'),
	pm25_drivers            : () => import('$lib/data/rio_grande_sul_br_sat_pm25_drivers_summary.json'),
	forestry_aptitude       : () => import('$lib/data/rio_grande_sul_br_sat_forestry_aptitude_summary.json'),
	agri_potential          : () => import('$lib/data/rio_grande_sul_br_sat_agri_potential_summary.json'),
	land_use                : () => import('$lib/data/rio_grande_sul_br_sat_land_use_summary.json'),
};

const TERRITORY_SUMMARIES: Record<string, Record<string, () => Promise<any>>> = {
	'itapua_py/': ITAPUA_SUMMARIES,
	'corrientes/': CORRIENTES_SUMMARIES,
	'alto_parana_py/': ALTO_PARANA_SUMMARIES,
	'chaco/': CHACO_SUMMARIES,
	'formosa/': FORMOSA_SUMMARIES,
	'parana_br/': PARANA_BR_SUMMARIES,
	'santa_catarina_br/': SANTA_CATARINA_BR_SUMMARIES,
	'rio_grande_sul_br/': RIO_GRANDE_SUL_BR_SUMMARIES,
};

export async function loadDeptSummary(analysisId: string, territoryPrefix: string): Promise<any> {
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
	return (summary.departments as any[])
		.map(d => ({ name: (d.dpto ?? d.distrito ?? d.municipio ?? '') as string, parquetKey: d.parquetKey as string }))
		.filter(d => d.name && d.parquetKey)
		.sort((a, b) => a.name.localeCompare(b.name));
}
