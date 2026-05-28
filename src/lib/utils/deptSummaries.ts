// Shared dept summary loaders — used by OvertureAnalysis and ComparisonPanel.
// Each loader returns the bundled JSON for a given analysis × territory.

const SAT_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk:   () => import('$lib/data/sat_environmental_risk_dept_summary.json'),
	climate_comfort:      () => import('$lib/data/sat_climate_comfort_dept_summary.json'),
	green_capital:        () => import('$lib/data/sat_green_capital_dept_summary.json'),
	change_pressure:      () => import('$lib/data/sat_change_pressure_dept_summary.json'),
	location_value:       () => import('$lib/data/sat_location_value_dept_summary.json'),
	agri_potential:       () => import('$lib/data/sat_agri_potential_dept_summary.json'),
	forest_health:        () => import('$lib/data/sat_forest_health_dept_summary.json'),
	forestry_aptitude:    () => import('$lib/data/sat_forestry_aptitude_dept_summary.json'),
	service_deprivation:  () => import('$lib/data/sat_service_deprivation_dept_summary.json'),
	territorial_isolation: () => import('$lib/data/sat_territorial_isolation_dept_summary.json'),
	health_access:        () => import('$lib/data/sat_health_access_dept_summary.json'),
	education_capital:    () => import('$lib/data/sat_education_capital_dept_summary.json'),
	education_flow:       () => import('$lib/data/sat_education_flow_dept_summary.json'),
	land_use:             () => import('$lib/data/sat_land_use_dept_summary.json'),
	territorial_types:    () => import('$lib/data/sat_territorial_types_dept_summary.json'),
	flood_risk:           () => import('$lib/data/flood_dept_summary.json'),
	territorial_scores:   () => import('$lib/data/scores_dept_summary.json'),
	sociodemographic:     () => import('$lib/data/sat_sociodemographic_dept_summary.json'),
	economic_activity:    () => import('$lib/data/sat_economic_activity_dept_summary.json'),
	accessibility:        () => import('$lib/data/sat_accessibility_dept_summary.json'),
	climate_vulnerability: () => import('$lib/data/sat_climate_vulnerability_dept_summary.json'),
	carbon_stock:         () => import('$lib/data/sat_carbon_stock_dept_summary.json'),
	pm25_drivers:         () => import('$lib/data/sat_pm25_drivers_dept_summary.json'),
	productive_activity:  () => import('$lib/data/sat_productive_activity_dept_summary.json'),
	deforestation_dynamics: () => import('$lib/data/sat_deforestation_dynamics_dept_summary.json'),
	soil_water:             () => import('$lib/data/sat_soil_water_dept_summary.json'),
};

const ITAPUA_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk:     () => import('$lib/data/itapua_py_sat_environmental_risk_summary.json'),
	climate_comfort:        () => import('$lib/data/itapua_py_sat_climate_comfort_summary.json'),
	green_capital:          () => import('$lib/data/itapua_py_sat_green_capital_summary.json'),
	change_pressure:        () => import('$lib/data/itapua_py_sat_change_pressure_summary.json'),
	forest_health:          () => import('$lib/data/itapua_py_sat_forest_health_summary.json'),
	deforestation_dynamics: () => import('$lib/data/itapua_py_sat_deforestation_dynamics_summary.json'),
	agri_potential:         () => import('$lib/data/itapua_py_sat_agri_potential_summary.json'),
	carbon_stock:           () => import('$lib/data/itapua_py_sat_carbon_stock_summary.json'),
	climate_vulnerability:  () => import('$lib/data/itapua_py_sat_climate_vulnerability_summary.json'),
	pm25_drivers:           () => import('$lib/data/itapua_py_sat_pm25_drivers_summary.json'),
	productive_activity:    () => import('$lib/data/itapua_py_sat_productive_activity_summary.json'),
	forestry_aptitude:      () => import('$lib/data/itapua_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/itapua_py_sat_land_use_summary.json'),
	accessibility:          () => import('$lib/data/itapua_py_sat_accessibility_summary.json'),
	flood_risk:             () => import('$lib/data/itapua_py_flood_dept_summary.json'),
	territorial_scores:     () => import('$lib/data/itapua_py_scores_dept_summary.json'),
	soil_water:             () => import('$lib/data/itapua_py_sat_soil_water_summary.json'),
};

const CORRIENTES_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk:     () => import('$lib/data/corrientes_sat_environmental_risk_summary.json'),
	climate_comfort:        () => import('$lib/data/corrientes_sat_climate_comfort_summary.json'),
	green_capital:          () => import('$lib/data/corrientes_sat_green_capital_summary.json'),
	change_pressure:        () => import('$lib/data/corrientes_sat_change_pressure_summary.json'),
	agri_potential:         () => import('$lib/data/corrientes_sat_agri_potential_summary.json'),
	forest_health:          () => import('$lib/data/corrientes_sat_forest_health_summary.json'),
	accessibility:          () => import('$lib/data/corrientes_sat_accessibility_summary.json'),
	carbon_stock:           () => import('$lib/data/corrientes_sat_carbon_stock_summary.json'),
	climate_vulnerability:  () => import('$lib/data/corrientes_sat_climate_vulnerability_summary.json'),
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
	productive_activity:    () => import('$lib/data/corrientes_sat_productive_activity_summary.json'),
	deforestation_dynamics: () => import('$lib/data/corrientes_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/corrientes_sat_forestry_aptitude_summary.json'),
	territorial_types:      () => import('$lib/data/corrientes_sat_territorial_types_summary.json'),
	location_value:         () => import('$lib/data/corrientes_sat_location_value_summary.json'),
};

// Alto Paraná (PY) — Phase 1: 6 core satellite analyses. More added per phase.
const ALTO_PARANA_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk:     () => import('$lib/data/alto_parana_py_sat_environmental_risk_summary.json'),
	climate_comfort:        () => import('$lib/data/alto_parana_py_sat_climate_comfort_summary.json'),
	green_capital:          () => import('$lib/data/alto_parana_py_sat_green_capital_summary.json'),
	change_pressure:        () => import('$lib/data/alto_parana_py_sat_change_pressure_summary.json'),
	forest_health:          () => import('$lib/data/alto_parana_py_sat_forest_health_summary.json'),
	agri_potential:         () => import('$lib/data/alto_parana_py_sat_agri_potential_summary.json'),
	// Phase 2 (8/11 done):
	carbon_stock:           () => import('$lib/data/alto_parana_py_sat_carbon_stock_summary.json'),
	climate_vulnerability:  () => import('$lib/data/alto_parana_py_sat_climate_vulnerability_summary.json'),
	deforestation_dynamics: () => import('$lib/data/alto_parana_py_sat_deforestation_dynamics_summary.json'),
	forestry_aptitude:      () => import('$lib/data/alto_parana_py_sat_forestry_aptitude_summary.json'),
	land_use:               () => import('$lib/data/alto_parana_py_sat_land_use_summary.json'),
	pm25_drivers:           () => import('$lib/data/alto_parana_py_sat_pm25_drivers_summary.json'),
	soil_water:             () => import('$lib/data/alto_parana_py_sat_soil_water_summary.json'),
	flood_risk:             () => import('$lib/data/alto_parana_py_flood_dept_summary.json'),
	territorial_scores:     () => import('$lib/data/alto_parana_py_scores_dept_summary.json'),
	accessibility:          () => import('$lib/data/alto_parana_py_sat_accessibility_summary.json'),
	productive_activity:    () => import('$lib/data/alto_parana_py_sat_productive_activity_summary.json'),
};

// v1.1 new territories — 5 core comparable layers (agri_potential deferred,
// pending c_clay methodology revision; see project_spatia_v11_rebaseline memory).
const CHACO_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk: () => import('$lib/data/chaco_sat_environmental_risk_summary.json'),
	climate_comfort:    () => import('$lib/data/chaco_sat_climate_comfort_summary.json'),
	green_capital:      () => import('$lib/data/chaco_sat_green_capital_summary.json'),
	change_pressure:    () => import('$lib/data/chaco_sat_change_pressure_summary.json'),
	forest_health:      () => import('$lib/data/chaco_sat_forest_health_summary.json'),
	carbon_stock            : () => import('$lib/data/chaco_sat_carbon_stock_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/chaco_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/chaco_scores_dept_summary.json'),
	soil_water              : () => import('$lib/data/chaco_sat_soil_water_summary.json'),
	flood_risk              : () => import('$lib/data/chaco_flood_dept_summary.json'),
	pm25_drivers            : () => import('$lib/data/chaco_sat_pm25_drivers_summary.json'),
	productive_activity     : () => import('$lib/data/chaco_sat_productive_activity_summary.json'),
	climate_vulnerability   : () => import('$lib/data/chaco_sat_climate_vulnerability_summary.json'),
};
const FORMOSA_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk: () => import('$lib/data/formosa_sat_environmental_risk_summary.json'),
	climate_comfort:    () => import('$lib/data/formosa_sat_climate_comfort_summary.json'),
	green_capital:      () => import('$lib/data/formosa_sat_green_capital_summary.json'),
	change_pressure:    () => import('$lib/data/formosa_sat_change_pressure_summary.json'),
	forest_health:      () => import('$lib/data/formosa_sat_forest_health_summary.json'),
	carbon_stock            : () => import('$lib/data/formosa_sat_carbon_stock_summary.json'),
	pm25_drivers            : () => import('$lib/data/formosa_sat_pm25_drivers_summary.json'),
	productive_activity     : () => import('$lib/data/formosa_sat_productive_activity_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/formosa_sat_deforestation_dynamics_summary.json'),
	soil_water              : () => import('$lib/data/formosa_sat_soil_water_summary.json'),
	territorial_scores      : () => import('$lib/data/formosa_scores_dept_summary.json'),
	flood_risk              : () => import('$lib/data/formosa_flood_dept_summary.json'),
};
const PARANA_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk: () => import('$lib/data/parana_br_sat_environmental_risk_summary.json'),
	climate_comfort:    () => import('$lib/data/parana_br_sat_climate_comfort_summary.json'),
	green_capital:      () => import('$lib/data/parana_br_sat_green_capital_summary.json'),
	change_pressure:    () => import('$lib/data/parana_br_sat_change_pressure_summary.json'),
	forest_health:      () => import('$lib/data/parana_br_sat_forest_health_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/parana_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/parana_br_scores_dept_summary.json'),
};
const SANTA_CATARINA_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk: () => import('$lib/data/santa_catarina_br_sat_environmental_risk_summary.json'),
	climate_comfort:    () => import('$lib/data/santa_catarina_br_sat_climate_comfort_summary.json'),
	green_capital:      () => import('$lib/data/santa_catarina_br_sat_green_capital_summary.json'),
	change_pressure:    () => import('$lib/data/santa_catarina_br_sat_change_pressure_summary.json'),
	forest_health:      () => import('$lib/data/santa_catarina_br_sat_forest_health_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/santa_catarina_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/santa_catarina_br_scores_dept_summary.json'),
	carbon_stock            : () => import('$lib/data/santa_catarina_br_sat_carbon_stock_summary.json'),
	pm25_drivers            : () => import('$lib/data/santa_catarina_br_sat_pm25_drivers_summary.json'),
	productive_activity     : () => import('$lib/data/santa_catarina_br_sat_productive_activity_summary.json'),
	flood_risk              : () => import('$lib/data/santa_catarina_br_flood_dept_summary.json'),
};
const RIO_GRANDE_SUL_BR_SUMMARIES: Record<string, () => Promise<any>> = {
	environmental_risk: () => import('$lib/data/rio_grande_sul_br_sat_environmental_risk_summary.json'),
	climate_comfort:    () => import('$lib/data/rio_grande_sul_br_sat_climate_comfort_summary.json'),
	green_capital:      () => import('$lib/data/rio_grande_sul_br_sat_green_capital_summary.json'),
	change_pressure:    () => import('$lib/data/rio_grande_sul_br_sat_change_pressure_summary.json'),
	forest_health:      () => import('$lib/data/rio_grande_sul_br_sat_forest_health_summary.json'),
	deforestation_dynamics  : () => import('$lib/data/rio_grande_sul_br_sat_deforestation_dynamics_summary.json'),
	territorial_scores      : () => import('$lib/data/rio_grande_sul_br_scores_dept_summary.json'),
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
