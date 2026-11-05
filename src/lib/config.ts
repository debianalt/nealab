import { unitLabel } from '$lib/utils/units';
import { MAP_CLASS_3, MAP_CLASS_4, MAP_CLASS_5, MAP_VIRIDIS_3, MAP_ACCESS_5, AGRI_APT_HEX, USO_HEX, MAP_SELVA, ZERO_HEX, NODATA_HEX, HARVEST_HEX } from '$lib/utils/sello';
import { i18n } from '$lib/stores/i18n.svelte';
import pm25Scale from '$lib/content/pm25_scale.json';
const R2_PROD = 'https://cdn.spatia.ar';

function getBase(): string {
	return R2_PROD;
}

export function getTilesUrl(name: 'buildings' | 'misiones_plantations' | 'corrientes_plantations' | 'chaco_plantations' | 'formosa_plantations' | 'itapua_buildings' | 'itapua_districts' | 'corrientes_buildings' | 'alto_parana_buildings' | 'alto_parana_districts' | 'chaco_buildings' | 'formosa_buildings' | 'parana_br_buildings' | 'santa_catarina_br_buildings' | 'rio_grande_sul_br_buildings' | 'radios' | 'terrain' | 'hexagons' | 'concepcion_py_buildings' | 'concepcion_py_districts' | 'san_pedro_py_buildings' | 'san_pedro_py_districts' | 'cordillera_py_buildings' | 'cordillera_py_districts' | 'guaira_py_buildings' | 'guaira_py_districts' | 'caaguazu_py_buildings' | 'caaguazu_py_districts' | 'caazapa_py_buildings' | 'caazapa_py_districts' | 'misiones_py_buildings' | 'misiones_py_districts' | 'paraguari_py_buildings' | 'paraguari_py_districts' | 'central_py_buildings' | 'central_py_districts' | 'neembucu_py_buildings' | 'neembucu_py_districts' | 'amambay_py_buildings' | 'amambay_py_districts' | 'canindeyu_py_buildings' | 'canindeyu_py_districts' | 'presidente_hayes_py_buildings' | 'presidente_hayes_py_districts' | 'boqueron_py_buildings' | 'boqueron_py_districts' | 'alto_paraguay_py_buildings' | 'alto_paraguay_py_districts'): string {
	if (name === 'terrain') {
		// Mapterhorn (Copernicus GLO-30 in the region) cut to TERRAIN_DEM.bounds, z0-11,
		// one object per tile so each is edge-cached (a single .pmtiles exceeds the 512 MB cache cap).
		return `${getBase()}/data/terrain/mapterhorn-v1/{z}/{x}/{y}.webp`;
	}
	const files: Record<string, string> = {
		buildings: 'tiles/buildings-v5.pmtiles',
		misiones_plantations: 'data/tiles/misiones_plantations_v4.pmtiles',   // v4: + km por la red hasta el asfalto (red_vial.py); v2: edad, situación de turno, dpto_key
		corrientes_plantations: 'data/tiles/corrientes_plantations_v4.pmtiles',   // v4: + km por la red hasta el asfalto (red_vial.py); v2: edad, situación de turno, dpto_key
		chaco_plantations: 'data/tiles/chaco_plantations_v4.pmtiles',   // v4: + km por la red hasta el asfalto (red_vial.py); v2: edad, situación de turno, dpto_key
		formosa_plantations: 'data/tiles/formosa_plantations_v4.pmtiles',   // v4: + km por la red hasta el asfalto (red_vial.py); v2: edad, situación de turno, dpto_key
		itapua_buildings: 'tiles/itapua_buildings-v4.pmtiles',
		itapua_districts: 'data/tiles/itapua_districts.pmtiles',
		corrientes_buildings: 'data/tiles/corrientes_buildings-v4.pmtiles',
		alto_parana_buildings: 'data/tiles/alto_parana_buildings-v4.pmtiles',
		alto_parana_districts: 'data/tiles/alto_parana_districts.pmtiles',
		chaco_buildings: 'data/tiles/chaco_buildings-v4.pmtiles',
		formosa_buildings: 'data/tiles/formosa_buildings-v4.pmtiles',
		parana_br_buildings: 'data/tiles/parana_br_buildings-v8.pmtiles',
		santa_catarina_br_buildings: 'data/tiles/santa_catarina_br_buildings-v8.pmtiles',
		rio_grande_sul_br_buildings: 'data/tiles/rio_grande_sul_br_buildings-v8.pmtiles',
		radios: 'data/tiles/radios-v5.pmtiles', // v5 (4-nov-2026): every radio in every tile it crosses, + dpto_key
		hexagons: 'tiles/hexagons-v2.pmtiles',
		// Paraguay departments — v1 (buildings from Overture, districts from GAUL)
		concepcion_py_buildings: 'data/tiles/concepcion_py_buildings.pmtiles',
		concepcion_py_districts: 'data/tiles/concepcion_districts.pmtiles',
		san_pedro_py_buildings: 'data/tiles/san_pedro_py_buildings.pmtiles',
		san_pedro_py_districts: 'data/tiles/san_pedro_districts.pmtiles',
		cordillera_py_buildings: 'data/tiles/cordillera_py_buildings.pmtiles',
		cordillera_py_districts: 'data/tiles/cordillera_districts.pmtiles',
		guaira_py_buildings: 'data/tiles/guaira_py_buildings.pmtiles',
		guaira_py_districts: 'data/tiles/guaira_districts.pmtiles',
		caaguazu_py_buildings: 'data/tiles/caaguazu_py_buildings.pmtiles',
		caaguazu_py_districts: 'data/tiles/caaguazu_districts.pmtiles',
		caazapa_py_buildings: 'data/tiles/caazapa_py_buildings.pmtiles',
		caazapa_py_districts: 'data/tiles/caazapa_districts.pmtiles',
		misiones_py_buildings: 'data/tiles/misiones_py_buildings.pmtiles',
		misiones_py_districts: 'data/tiles/misiones_districts.pmtiles',
		paraguari_py_buildings: 'data/tiles/paraguari_py_buildings.pmtiles',
		paraguari_py_districts: 'data/tiles/paraguari_districts.pmtiles',
		central_py_buildings: 'data/tiles/central_py_buildings.pmtiles',
		central_py_districts: 'data/tiles/central_districts.pmtiles',
		neembucu_py_buildings: 'data/tiles/neembucu_py_buildings.pmtiles',
		neembucu_py_districts: 'data/tiles/neembucu_districts.pmtiles',
		amambay_py_buildings: 'data/tiles/amambay_py_buildings.pmtiles',
		amambay_py_districts: 'data/tiles/amambay_districts.pmtiles',
		canindeyu_py_buildings: 'data/tiles/canindeyu_py_buildings.pmtiles',
		canindeyu_py_districts: 'data/tiles/canindeyu_districts.pmtiles',
		presidente_hayes_py_buildings: 'data/tiles/presidente_hayes_py_buildings.pmtiles',
		presidente_hayes_py_districts: 'data/tiles/presidente_hayes_districts.pmtiles',
		boqueron_py_buildings: 'data/tiles/boqueron_py_buildings.pmtiles',
		boqueron_py_districts: 'data/tiles/boqueron_districts.pmtiles',
		alto_paraguay_py_buildings: 'data/tiles/alto_paraguay_py_buildings.pmtiles',
		alto_paraguay_py_districts: 'data/tiles/alto_paraguay_districts.pmtiles',
	};
	return `pmtiles://${getBase()}/${files[name]}`;
}

export const CHM_COLORS: Record<number, string> = {
	0: '#15803d', // Selva alta continua
	1: '#65a30d', // Mosaico agro-forestal
	2: '#ca8a04', // Agrícola/pasturas
	3: '#6b7280', // Urbano/periurbano
} as const;

export const CHM_LABELS: Record<number, string> = {
	0: 'Selva alta continua',
	1: 'Mosaico agro-forestal',
	2: 'Agrícola/pasturas',
	3: 'Urbano/periurbano',
} as const;

// Visualisation only: no analysis reads this DEM (slope, TWI, HAND come from the pipeline).
export const TERRAIN_DEM = {
	tileSize: 512,
	maxzoom: 11,
	bounds: [-64, -34, -47.5, -19] as [number, number, number, number],
	attribution:
		'Relieve: <a href="https://mapterhorn.com/attribution" target="_blank" rel="noopener">© Mapterhorn</a> · Copernicus GLO-30 © DLR e.V. 2010-2014, © Airbus Defence and Space GmbH 2014-2018, provided under COPERNICUS by the European Union and ESA'
};

export const TERRAIN_CONFIG = {
	exaggeration: 1.5,
	hillshade: {
		shadowColor: '#0a0a1a',
		highlightColor: '#8899bb',
		illuminationDirection: 315,
		exaggeration: 0.7
	}
} as const;

export function getParquetUrl(name: string): string {
	const busts: Record<string, string> = {
		hex_flood_risk: '?v=41',   // 4-nov-2026: sin flood_risk_score
		sat_agri_apt: '?v=1',
		sat_forestry_aptitude: '?v=40',
		sat_service_deprivation: '?v=32',
		sat_health_access: '?v=30',
		sat_education_capital: '?v=30',
		sat_education_flow: '?v=29',
		sat_sociodemographic: '?v=27',   // 4-nov-2026: Corrientes + 28.361 celdas censales que faltaban (fill_census_rows.py)
		sat_economic_activity: '?v=37',   // 37: restaurado v35 (grilla completa, relleno de Corrientes, transiciones) + las 2 columnas; la 36 salió de una copia del 2-nov (5-nov-2026); 36: ocupados_est y tasa_desocupacion por celda (add_econ_hex_derived.py, 5-nov-2026); 35: universitarios 2010 con respaldo areal; 34 (4-nov-2026, noche): pct_universitario_2010, viirs_2014[_neto], viirs_2025_neto (build_econ_trayectorias.py); 4-nov: Corrientes, tasas en 49.425 celdas vacías (fill_census_rows.py); 2-nov: VIIRS 2025, grilla completa
		sat_accessibility: '?v=32',   // 5-nov-2026: todo en minutos por camino (hospital, salud, secundaria, ruta)
		sat_pm25_drivers: '?v=20',   // 4-nov-2026: sin score*
		sat_deforestation_dynamics: '?v=20',   // 20: c_loss_pct_forest(_baseline/_delta) por celda (5-nov-2026); 4-nov-2026: sin score* ni tipos; 3-nov-2026: Hansen v1.13, 2016–2025 (+ Rio Grande do Sul)
		sat_uso_suelo: '?v=1',  // 3-nov-2026: MapBiomas AR Col. 3 (reemplaza Dynamic World)
		sat_censo_temporal: '?v=1',
		sat_eudr: '?v=1',
	};
	const bust = busts[name] || '';
	return `${getBase()}/data/${name}.parquet${bust}`;
}

// Bump DEPT_V after any pipeline run that regenerates dept-split parquets.
// Cloudflare CDN caches stable versioned URLs; a new version invalidates naturally.
const DEPT_V = 43;   // 43: deforestation_dynamics con % del bosque por celda (5-nov-2026); 42: economic_activity restaurado (la 41 sirvió una copia del 2-nov unas horas, 5-nov-2026); 41: economic_activity con ocupados_est y tasa_desocupacion (5-nov-2026); 40: accesibilidad en minutos por camino (5-nov-2026); 38: education_capital con Censo 2010 y economic_activity con el respaldo areal 2010 (4-nov-2026); 37: economic_activity con columnas de año base

export function getFloodDptoUrl(parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}flood_dpto/hex_flood_${parquetKey}.parquet?v=${DEPT_V}`;
}

// INDEC 2022 radios of an AR territory as a unit of their own (pipeline/build_census_radios.py):
// rates, bases, queen neighbours and the cells that show each radio. Only AR has radios.
export const CENSUS_RADIOS_V = 1;
export function getCensusRadiosUrl(territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}census_radios_2022.parquet?v=${CENSUS_RADIOS_V}`;
}

// People estimated per cell from its buildings; per radio they add up to the census population
// (pipeline/build_cell_population.py). Read by the click selection by radio.
export const CELL_POPULATION_V = 1;
export function getCellPopulationUrl(territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}cell_population_2022.parquet?v=${CELL_POPULATION_V}`;
}

export function getSatDptoUrl(analysisId: string, parquetKey: string, territoryPrefix = ''): string {
	// File stem = the layer's parquet name: sat_<id> for every layer except agri_potential,
	// whose empirical version (physical units + crop suitability) ships as sat_agri_apt.
	const stem = HEX_LAYER_REGISTRY[analysisId]?.parquet ?? `sat_${analysisId}`;
	return `${getBase()}/data/${territoryPrefix}sat_dpto/${stem}_${parquetKey}.parquet?v=${DEPT_V}`;
}

// EUDR refresh manifest — written by pipeline/combine_eudr_parquets.py on each
// monthly CI run ({refreshed, hansen_version, vintage, units}). Daily cache-buster:
// the manifest must track the refresh, not sit in the edge cache.
export function getEudrMetaUrl(): string {
	return `${getBase()}/data/eudr/meta.json?d=${new Date().toISOString().slice(0, 10)}`;
}

// EUDR hi-res (res-9) deforestation grid — single combined parquet, sorted by
// h3index with row-group stats so DuckDB-WASM range-reads only the matching group.
// v=3: Hansen v1.13 (loss through 2025, adds loss_2025_pct) + ZSTD compression.
export function getEudrHiresUrl(): string {
	// v=5: adds loss_2025_pct (AR) so the check page's LOSS_YEARS query binds; v=4
	// had Hansen 2025 + fire but the yearly column was dropped in the combine.
	return `${getBase()}/data/eudr/hires/eudr_res9_combined.parquet?v=6`;
}

// Plantation vs native-forest fraction per res-9 hex (MapBiomas class 9 / 3-6).
// Joined by h3index in /eudr/check to flag plantation harvest cycles that Hansen
// mislabels as deforestation. AR-only for now (MapBiomas AR Collection 2);
// PY/BR have no usable plantation class → those hexes are simply absent (sin dato).
export function getEudrPlantationUrl(): string {
	// v=5: MapBiomas Coleccion 2 (lectura 2020 y 2024) a 30 m, todos los pixeles.
	return `${getBase()}/data/eudr/hires/eudr_plantation_res9.parquet?v=5`;
}

// res-7 plantation (matches the main-map EUDR layer's res-7 grid) — used to show
// the plantation/native breakdown when an admin-2 unit is selected on the main map.
// v=2: split bosque (clase 3) vs monte/sabana (clase 4+6) + 4 NEA provincias.
export function getEudrPlantationRes7Url(): string {
	// v=5: MapBiomas Coleccion 2 (lectura 2020 y 2024) a 30 m, asignacion directa a res-7.
	return `${getBase()}/data/eudr/hires/eudr_plantation_res7.parquet?v=5`;
}

export function getSatGlobalUrl(analysisId: string, territoryPrefix = ''): string {
	const layer = HEX_LAYER_REGISTRY[analysisId];
	const name = layer?.parquet ?? `sat_${analysisId}`;
	const base = getParquetUrl(name); // inherits static ?v=N from busts map
	if (!territoryPrefix) return base;
	return base.replace('/data/', `/data/${territoryPrefix}`);
}

// Department-level exact INDEC totals for censo_temporal (build_censo_temporal_dept.py).
// Tiny parquet (~9-25 rows); dept_code maps to `redcode` in ar_dept_boundaries.json.
export function getCensoTemporalDeptUrl(territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}sat_censo_temporal_dept.parquet?v=1`;
}

export function getDeptSummaryUrl(analysisId: string, territoryPrefix = ''): string {
	const nameMap: Record<string, string> = {
		flood_risk: 'flood_dept_summary',
	};
	const name = nameMap[analysisId] ?? `sat_${analysisId}_dept_summary`;
	return `${getBase()}/data/${territoryPrefix}${name}.json`;
}

// BR municipality boundaries: GADM_BRA_2 simplified per state. Fetched on demand
// (Option B) instead of bundled — see pipeline/build_br_districts.py.
export function getBrDistrictsUrl(territoryId: string): string {
	// ?v bump on hi-res re-export (0.003→0.0008) to bust the CF edge cache.
	return `${getBase()}/data/br_districts/${territoryId}_districts.geojson?v=2`;
}

export const PARQUETS = {
	get censo_radios() { return getParquetUrl('censo_radios'); },
	get censo_departamentos() { return getParquetUrl('censo_departamentos'); },
	get magyp_estimaciones() { return getParquetUrl('magyp_estimaciones'); },
	get ndvi_annual() { return getParquetUrl('ndvi_annual'); },
	get buildings_stats() { return getParquetUrl('buildings_stats'); },
	get radio_stats_master() { return getParquetUrl('radio_stats_master'); },
	get radio_stats_corrientes() { return `${getBase()}/data/corrientes/radio_stats_corrientes.parquet`; },
	get radio_stats_chaco() { return `${getBase()}/data/chaco/radio_stats_chaco.parquet`; },
	get radio_stats_formosa() { return `${getBase()}/data/formosa/radio_stats_formosa.parquet`; },
	get hex_flood_risk() { return getParquetUrl('hex_flood_risk'); },
	get h3_radio_crosswalk() { return getParquetUrl('h3_radio_crosswalk'); },
	get h3_parent_crosswalk() { return getParquetUrl('h3_parent_crosswalk'); },
	// Satellite composite scores
	get sat_agri_apt() { return getParquetUrl('sat_agri_apt'); },
	get sat_forestry_aptitude() { return getParquetUrl('sat_forestry_aptitude'); },
	get sat_service_deprivation() { return getParquetUrl('sat_service_deprivation'); },
	get sat_uso_suelo() { return getParquetUrl('sat_uso_suelo'); },
	get sat_health_access() { return getParquetUrl('sat_health_access'); },
	get sat_education_capital() { return getParquetUrl('sat_education_capital'); },
	get sat_education_flow() { return getParquetUrl('sat_education_flow'); },
	get sat_sociodemographic() { return getParquetUrl('sat_sociodemographic'); },
	get sat_economic_activity() { return getParquetUrl('sat_economic_activity'); },
	get sat_accessibility() { return getParquetUrl('sat_accessibility'); },
	get sat_censo_temporal() { return getParquetUrl('sat_censo_temporal'); },
	// Brazil IBGE Censo 2022 setor-level stats
	get radio_stats_parana_br()         { return `${getBase()}/data/setores_stats_parana_br.parquet`; },
	get radio_stats_santa_catarina_br() { return `${getBase()}/data/setores_stats_santa_catarina_br.parquet`; },
	get radio_stats_rio_grande_sul_br() { return `${getBase()}/data/setores_stats_rio_grande_sul_br.parquet`; },
	// EUDR post-2020 loss, res-9 per territory (dept splits in {prefix}sat_dpto/)
	get sat_eudr() { return getParquetUrl('sat_eudr'); },
	// Itapua district-level satellite scores
	get district_stats_itapua() { return `${getBase()}/data/itapua_py/district_stats_itapua.parquet`; },
	// Alto Paraná district-level satellite scores
	get district_stats_alto_parana() { return `${getBase()}/data/alto_parana_py/district_stats_alto_parana.parquet`; },
};

export const BASEMAP = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
// Per theme. Positron and Dark Matter share the 95 layer ids, sources, filters and
// layout (verified 1-nov-2026): a theme change repaints the mounted layers
// (utils/basemapTheme.ts) instead of setStyle, which would wipe Spatia's own layers.
export const BASEMAP_STYLE = {
	claro: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
	oscuro: BASEMAP,
} as const;

export const MAP_INIT = {
	center: [-56.7, -28.1] as [number, number],  // NEA regional view
	zoom: 6.0,
	pitch: 50,
	bearing: -15,
	minZoom: 5,
	maxZoom: 18
} as const;

export const MAP_PROVINCE = {
	center: [-54.4, -27.0] as [number, number],
	zoom: 7.5,
	pitch: 50,
	bearing: -15,
} as const;

export const COLOR_RAMPS = {
	population: {
		property: 'est_personas',
		// Stops expanded across the low-mid residential range (1-12 ppl/building),
		// where the bulk of the urban fabric sits across ALL territories. The old
		// ramp reserved brightness for 10-50, only reached by coarse district-level
		// allocation spikes (PY), leaving finely-allocated cities (BR setores, AR
		// radios) reading near-black despite being densely populated. Still absolute
		// → cross-territory comparable.
		stops: [0, '#0f1e30', 1, '#26527a', 2, '#3a7bb0', 4, '#509cd6', 7, '#74bef5', 12, '#a3d8ff', 25, '#d6f1ff'],
		legendTitleKey: 'legend.estPersons',
		legendLabels: ['0', '1', '4', '7', '12', '25+']
	},
	volume: {
		// Built volume = footprint area × height (m³). Direct GBA measurement with the
		// SAME method in all 9 territories — no census-allocation granularity artifact,
		// so brightness is objectively comparable (big/tall buildings glow everywhere,
		// matching real built intensity). est_personas stays as click-through data.
		// Log-spaced because volume is long-tailed (houses ~500 m³, towers ~60k m³).
		property: 'volume_m3',
		stops: [150, '#0f1e30', 500, '#26527a', 1500, '#3a7bb0', 4000, '#509cd6', 10000, '#74bef5', 25000, '#a3d8ff', 60000, '#d6f1ff'],
		legendTitleKey: 'legend.buildingVolume',
		legendLabels: ['<0.5k', '1.5k', '4k', '10k', '25k', '60k+ m³']
	},
	height: {
		property: 'best_height_m',
		stops: [3, '#0f1e30', 5, '#1e4060', 8, '#3080c0', 12, '#50a0e0', 20, '#70c0ff', 35, '#a0dfff', 50, '#d0f0ff'],
		legendTitleKey: 'legend.buildingHeight',
		legendLabels: ['3m', '5m', '8m', '12m', '20m', '35m+']
	}
} as const;

export const GRADIENT_CSS = 'linear-gradient(to right, #0f1e30, #1e4060, #3080c0, #50a0e0, #70c0ff, #a0dfff, #d0f0ff)';

// ── Territory system ──────────────────────────────────────────────────────────

export type CountryId = 'ar' | 'py' | 'br';

export interface TerritoryConfig {
	id: string;
	label: string;
	shortLabel: string;
	country: CountryId;
	bbox: [number, number, number, number]; // [W, S, E, N]
	parquetPrefix: string; // '' for Misiones, 'itapua_py/' for Itapúa, etc.
	flag: string;
	available: boolean;
}

export const TERRITORY_REGISTRY: Record<string, TerritoryConfig> = {
	misiones: {
		id: 'misiones', label: 'Misiones', shortLabel: 'MIS', country: 'ar',
		bbox: [-56.10, -28.20, -53.55, -25.44],
		parquetPrefix: '', flag: '🇦🇷', available: true,
	},
	itapua_py: {
		id: 'itapua_py', label: 'Itapúa', shortLabel: 'ITA', country: 'py',
		bbox: [-57.40, -27.70, -55.00, -26.40],  // Full Itapúa department
		parquetPrefix: 'itapua_py/', flag: '🇵🇾', available: true,
	},
	alto_parana_py: {
		id: 'alto_parana_py', label: 'Alto Paraná', shortLabel: 'APY', country: 'py',
		bbox: [-55.65, -26.36, -54.25, -24.38],  // derived from INE district dissolve (Phase 0)
		parquetPrefix: 'alto_parana_py/', flag: '🇵🇾', available: true,
	},
	// ── Argentina — próximamente ─────────────────────────────────────────────
	corrientes: {
		id: 'corrientes', label: 'Corrientes', shortLabel: 'CTE', country: 'ar',
		bbox: [-59.80, -30.80, -55.60, -27.20],
		parquetPrefix: 'corrientes/', flag: '🇦🇷', available: true,
	},
	chaco: {
		id: 'chaco', label: 'Chaco', shortLabel: 'CHA', country: 'ar',
		bbox: [-63.50, -28.00, -58.40, -24.80],  // synced with pipeline/config.py + config_eudr.py
		parquetPrefix: 'chaco/', flag: '🇦🇷', available: true,
	},
	formosa: {
		id: 'formosa', label: 'Formosa', shortLabel: 'FOR', country: 'ar',
		bbox: [-62.20, -27.00, -57.40, -22.90],  // synced with pipeline/config.py + config_eudr.py
		parquetPrefix: 'formosa/', flag: '🇦🇷', available: true,
	},
	// ── Paraguay — todos los departamentos ──────────────────────────────────
	// GAUL-based boundaries. District tiles deployed; satellite data pending GEE.
	concepcion_py: {
		id: 'concepcion_py', label: 'Concepción', shortLabel: 'CON', country: 'py',
		bbox: [-62.3, -23.8, -55.4, -20.3],
		parquetPrefix: 'concepcion_py/', flag: '🇵🇾', available: true,
	},
	san_pedro_py: {
		id: 'san_pedro_py', label: 'San Pedro', shortLabel: 'SPE', country: 'py',
		bbox: [-59.4, -25.5, -55.3, -22.3],
		parquetPrefix: 'san_pedro_py/', flag: '🇵🇾', available: true,
	},
	cordillera_py: {
		id: 'cordillera_py', label: 'Cordillera', shortLabel: 'COR', country: 'py',
		bbox: [-57.8, -25.4, -56.6, -24.4],
		parquetPrefix: 'cordillera_py/', flag: '🇵🇾', available: true,
	},
	guaira_py: {
		id: 'guaira_py', label: 'Guairá', shortLabel: 'GUA', country: 'py',
		bbox: [-56.7, -26.1, -55.4, -25.1],
		parquetPrefix: 'guaira_py/', flag: '🇵🇾', available: true,
	},
	caaguazu_py: {
		id: 'caaguazu_py', label: 'Caaguazú', shortLabel: 'CAA', country: 'py',
		bbox: [-56.7, -25.9, -54.2, -23.3],
		parquetPrefix: 'caaguazu_py/', flag: '🇵🇾', available: true,
	},
	caazapa_py: {
		id: 'caazapa_py', label: 'Caazapá', shortLabel: 'CAZ', country: 'py',
		bbox: [-56.7, -26.9, -55.0, -25.5],
		parquetPrefix: 'caazapa_py/', flag: '🇵🇾', available: true,
	},
	misiones_py: {
		id: 'misiones_py', label: 'Misiones', shortLabel: 'MPY', country: 'py',
		bbox: [-57.5, -27.3, -56.3, -26.7],
		parquetPrefix: 'misiones_py/', flag: '🇵🇾', available: true,
	},
	paraguari_py: {
		id: 'paraguari_py', label: 'Paraguarí', shortLabel: 'PAR', country: 'py',
		bbox: [-58.1, -26.7, -56.5, -25.1],
		parquetPrefix: 'paraguari_py/', flag: '🇵🇾', available: true,
	},
	central_py: {
		id: 'central_py', label: 'Central', shortLabel: 'CEN', country: 'py',
		bbox: [-58.2, -25.8, -57.1, -24.9],
		parquetPrefix: 'central_py/', flag: '🇵🇾', available: true,
	},
	neembucu_py: {
		id: 'neembucu_py', label: 'Ñeembucú', shortLabel: 'NEE', country: 'py',
		bbox: [-58.6, -27.6, -57.1, -26.3],
		parquetPrefix: 'neembucu_py/', flag: '🇵🇾', available: true,
	},
	amambay_py: {
		id: 'amambay_py', label: 'Amambay', shortLabel: 'AMA', country: 'py',
		bbox: [-56.6, -23.7, -55.0, -21.7],
		parquetPrefix: 'amambay_py/', flag: '🇵🇾', available: true,
	},
	canindeyu_py: {
		id: 'canindeyu_py', label: 'Canindeyú', shortLabel: 'CAN', country: 'py',
		bbox: [-55.7, -25.3, -53.9, -22.6],
		parquetPrefix: 'canindeyu_py/', flag: '🇵🇾', available: true,
	},
	presidente_hayes_py: {
		id: 'presidente_hayes_py', label: 'Presidente Hayes', shortLabel: 'PHY', country: 'py',
		bbox: [-62.1, -25.6, -57.4, -21.9],
		parquetPrefix: 'presidente_hayes_py/', flag: '🇵🇾', available: true,
	},
	boqueron_py: {
		id: 'boqueron_py', label: 'Boquerón', shortLabel: 'BOQ', country: 'py',
		bbox: [-62.6, -23.2, -59.4, -20.0],
		parquetPrefix: 'boqueron_py/', flag: '🇵🇾', available: true,
	},
	alto_paraguay_py: {
		id: 'alto_paraguay_py', label: 'Alto Paraguay', shortLabel: 'AGP', country: 'py',
		bbox: [-61.2, -23.2, -57.4, -18.4],
		parquetPrefix: 'alto_paraguay_py/', flag: '🇵🇾', available: true,
	},
	// ── Brasil — próximamente ────────────────────────────────────────────────
	parana_br: {
		id: 'parana_br', label: 'Paraná', shortLabel: 'PR', country: 'br',
		bbox: [-54.60, -26.70, -48.00, -22.50],
		parquetPrefix: 'parana_br/', flag: '🇧🇷', available: true,
	},
	santa_catarina_br: {
		id: 'santa_catarina_br', label: 'Santa Catarina', shortLabel: 'SC', country: 'br',
		bbox: [-53.90, -29.40, -48.30, -25.90],
		parquetPrefix: 'santa_catarina_br/', flag: '🇧🇷', available: true,
	},
	rio_grande_sul_br: {
		id: 'rio_grande_sul_br', label: 'Rio Grande do Sul', shortLabel: 'RS', country: 'br',
		bbox: [-57.70, -33.80, -49.60, -27.00],
		parquetPrefix: 'rio_grande_sul_br/', flag: '🇧🇷', available: true,
	},
};

export function getDefaultTerritory(): TerritoryConfig {
	return TERRITORY_REGISTRY['misiones'];
}

export function getTerritoriesByCountry(): Record<CountryId, TerritoryConfig[]> {
	const result: Record<CountryId, TerritoryConfig[]> = { ar: [], py: [], br: [] };
	for (const t of Object.values(TERRITORY_REGISTRY)) {
		result[t.country].push(t);
	}
	return result;
}

// ── Lens system ──────────────────────────────────────────────────────────────

export type LensId = 'ambiente' | 'produccion' | 'poblacion' | 'economia';

export interface LensConfig {
	label: Record<'es' | 'en' | 'gn' | 'pt', string>;
	color: string;
}

export const LENS_CONFIG: Record<LensId, LensConfig> = {
	ambiente:   { label: { es: 'Ambiente y riesgo',          en: 'Environment & risk',        gn: 'Ambiente y riesgo', pt: 'Ambiente e risco'          }, color: '#ef4444' },
	produccion: { label: { es: 'Producción y suelo',         en: 'Production & land',         gn: "Mba'apo",    pt: 'Produção e solo'            }, color: '#22c55e' },
	poblacion:  { label: { es: 'Población y servicios',      en: 'Population & services',     gn: 'Tetã kuéra', pt: 'População e serviços'       }, color: '#3b82f6' },
	economia:   { label: { es: 'Economía y comercio',        en: 'Economy & trade',           gn: 'Moĩ viru',   pt: 'Economia e comércio'        }, color: '#f59e0b' },
} as const;

// ── Hex layer system (multi-resolution) ──────────────────────────────────────

export interface HexVariable {
	col: string;
	labelKey: string;
	aggregation: 'mean' | 'sum' | 'max';
	rawCol?: string;
	unit?: string;
	rawUnit?: string; // unit of the _raw physical value when it differs from `unit` (e.g. percentil display, hab/km² or min raw)
	scale?: number;   // multiply the stored value for display (e.g. DW land-cover fractions 0–1 → % with scale 100)
	baseCol?: string; // column with the households/people a % is computed over (e.g. pob_14_mas), shown in the hex panel's table
	hideIfZero?: boolean;
	// Loaded with the layer (the map can paint it) but not listed in the hex panel,
	// zone comparison: agri suitability classes 1–4 (S1…N).
	hidden?: boolean;
	// Listed in the panel table but left out of the hex panel's flower (agri: the 9
	// «% campañas aptas» would turn it into 19 unreadable petals).
	noPetal?: boolean;
}

export type TemporalMode = 'current' | 'baseline' | 'delta';

export interface HexLayerConfig {
	id: string;
	parquet: string;
	variables: HexVariable[];
	primaryVariable: string;
	colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night';
	aggregation: 'mean' | 'sum' | 'max';
	petalVars?: HexVariable[];
	titleKey: string;
	perDepartment?: boolean;
	temporal?: boolean;
	temporalPeriods?: { current: string; baseline: string; source?: string };
	coverage?: Record<string, 'available' | 'pending' | 'unavailable'>;
	legendLowKey?: string;
	legendHighKey?: string;
	// NOTE: cross-territory comparability is declared ONLY on ANALYSIS_REGISTRY
	// (`comparable: true`) — that is the single source of truth the UI reads. Do not
	// re-add a `comparable` field here; a stale duplicate previously implied (wrongly)
	// that only `accessibility` was comparable.
	// Fixed color-scale domain [lo, hi] in the primaryVariable's physical units.
	// When set, ensureColorDomain() uses it directly instead of querying MIN/MAX on
	// the global parquet — avoids a slow remote scan (carbon: 1 row-group, 2.7 MB
	// column = 3.6 s) AND makes colors identical cross-territory (more comparable).
	fixedColorDomain?: [number, number];
	// Color from P2 to P98 of the painted column instead of MIN–MAX: real census
	// rates are skewed (NBI 0–100 %, most hexagons under 30 %).
	quantileDomain?: boolean;
	// Discrete color classes on the primaryVariable, replacing the continuous ramp
	// (map and legend). Ignored in temporal delta mode.
	colorClasses?: ColorClasses;
	// Classes for other variables the map can paint instead of the primary
	// (hexStore.colorVariable, set from the active ranking tab).
	variableClasses?: Record<string, ColorClasses>;
	// The summaries carry a per-department and province series (serie: {anios, ha: {clase: number[]}}),
	// drawn by UsoSerie.svelte as «Evolución» (land use, 1985–2025).
	classSeries?: boolean;
	// Drawn as DNDFI plantation polygons (data/tiles/<t>_plantations_v4.pmtiles), not hexagons: no per-hex parquet;
	// the active tab's hexVariable names the tile property painted with that variable's classes (forestry_harvest).
	macizos?: boolean;
	// Legend end labels for those other variables (i18n keys), when the layer's
	// legendLowKey/HighKey describe only the primary.
	variableLegend?: Record<string, { low: string; high: string }>;
	// Two color scales, switched from the legend (URL ?escala=regional):
	// 'provincial' (default) = P2–P98 of the active territory from scaleStats —
	// the province/department/state is the governing unit; 'regional' = this
	// fixed range, so one color means one value in every territory.
	regionalColorDomain?: [number, number];
	scaleStats?: ScaleStats;
	// FlowChart banding over a RAW variable (raw-data-first convention). When
	// absent, FlowChart falls back to score/score_baseline with 33/67 cuts.
	flowBands?: FlowBands;
	// Transition chart per ranking tab, keyed by the tab's hexVariable (economic_activity: university 2010→2022,
	// night light 2014→2025). The baseline columns ride as hidden variables so the department load brings them.
	tabFlows?: Record<string, { bands: FlowBands; periods: { baseline: string; current: string } }>;
	// Department ranking shown first at province level: bars over a physical
	// metric read from each department's `rank` block in the dept summary JSON
	// (written by the pipeline), highest first. Absent → plain department list.
	deptRank?: DeptRank;
}

export interface DeptRank {
	field: string;        // key inside dept.rank (e.g. 'flood20_pct')
	unit: string;         // shown after the value
	decimals: number;
	titleKey: string;     // the question the ranking answers
	noteKey: string;      // one-line definition of the metric
	extraField?: string;  // optional second value (e.g. hectares), shown dimmed
	extraUnitKey?: string; // i18n key
	extraDecimals?: number;
	saturatedKey?: string; // i18n note shown instead of bars when the summary flags metadata.rank.saturated
	unitKey?: string;     // i18n unit, preferred over `unit` when set
	signed?: boolean;     // values can be negative: bar by |value|, green below 0, "+" shown
	// Several readings of the same layer, switched from tabs above the ranking; each
	// entry overrides the base fields. The base itself is the first reading.
	metrics?: Array<Partial<Omit<DeptRank, 'metrics'>> & { tabKey: string }>;
	// Per-hex column the map paints while this reading is active (needs
	// HexLayerConfig.variableClasses for it). Flood: the relief variables.
	// Key inside the summary's metadata.rank whose `province` block (territory-wide
	// value + label) is shown above the bars, or instead of them when no department
	// has a value (order not stable). Change tab: 'change'.
	province?: string;
	hexVariable?: string;
	// The reading exists only by department (no per-hex column): the panel says so and
	// names what the hexagons show instead.
	hexNone?: boolean;
	// The reading is the layer's own temporal change (deforestation, Aceleración): while the tab
	// is active the map runs in this temporal mode and the Actual / Línea base / Cambio toggle hides.
	temporalMode?: TemporalMode;
	// Macizo layers (forestry_harvest): the stands this reading is about (genus and situation codes, as in the
	// ranking). The map paints them in their class color and the rest gray and faint; the legend lists only them.
	// null in a tab = no focus (overrides the base).
	macizoFocus?: MacizoFocus | null;
}

// genero 'otras' = neither Pinus nor Eucalyptus. Year ranges are inclusive [from, to] over tile properties
// (anio_plant, anio_plant + turno, anio_perdida). label: a chart pick names itself in the legend.
export interface MacizoFocus {
	genero?: string; sit?: number[];
	anioPlant?: [number, number]; anioTurno?: [number, number]; anioPerdida?: [number, number];
	bandaRed?: number[];   // band of km by road network to the nearest paved road (tiles v4, banda_red; see harvest.chart.dis.t*)
	label?: string;
}

export interface ColorClass {
	from: number;      // class applies to value >= from (ascending; first class is the floor)
	color: string;
	labelKey: string;
}

export interface ColorClasses {
	// value === 0 is a measured value with its own class, not "Sin cobertura".
	zero?: { color: string; labelKey: string };
	// |value| below this also takes the zero class (fractional building counts:
	// under half a building reads as none).
	zeroBelow?: number;
	steps: ColorClass[];
	// true → values are class codes (each step.from is one class): the map tooltip
	// shows the class label instead of the number (agri suitability S1…N).
	ordinal?: boolean;
	// true → class codes with no order (land use: monte, plantación…): tooltip shows the label (as ordinal), the
	// coarse LOD cell takes the most frequent class instead of the mean code, and the value charts (histogram,
	// Moran, bivariate) are not drawn over codes.
	nominal?: boolean;
	// true → a cut belongs to the class below (value > from), matching FlowChart
	// bands; default → to the class above (value >= from).
	upperInclusive?: boolean;
	// Unit printed next to the legend title when the class labels use a different
	// one than the variable (avoid it: legend, tooltip and panel share one unit).
	unit?: string;
}

export function colorForClass(value: number, classes: ColorClasses): string {
	if (classes.zero && (value === 0 || (classes.zeroBelow !== undefined && Math.abs(value) < classes.zeroBelow))) return classes.zero.color;
	let color = classes.steps[0].color;
	for (const s of classes.steps) if (classes.upperInclusive ? value > s.from : value >= s.from) color = s.color;
	return color;
}

// Crop suitability (pipeline/hexoma, A4): median class of the 1991/92–2024/25 campaigns,
// 1 S1 optimum … 4 N not suitable (FAO, most limiting factor). Colors: Tierra colorada.
export const AGRI_APT_CLASSES: ColorClasses = {
	ordinal: true,
	steps: [
		{ from: 1, color: AGRI_APT_HEX[0], labelKey: 'agri.class.s1' },
		{ from: 2, color: AGRI_APT_HEX[1], labelKey: 'agri.class.s2' },
		{ from: 3, color: AGRI_APT_HEX[2], labelKey: 'agri.class.s3' },
		{ from: 4, color: AGRI_APT_HEX[3], labelKey: 'agri.class.n' },
	],
};

// Land use (pipeline/hexoma/exportar_uso_web.py, MapBiomas Argentina Col. 3): class with ≥ 50 % of the cell,
// 9 = mosaico (none reaches half). Colors: USO_HEX (sello.ts).
export const USO_CLASSES: ColorClasses = {
	nominal: true,
	steps: [
		{ from: 1, color: USO_HEX[0], labelKey: 'uso.class.monte' },
		{ from: 2, color: USO_HEX[1], labelKey: 'uso.class.plantacion' },
		{ from: 3, color: USO_HEX[2], labelKey: 'uso.class.perenne' },
		{ from: 4, color: USO_HEX[3], labelKey: 'uso.class.agro' },
		{ from: 5, color: USO_HEX[4], labelKey: 'uso.class.pastizal' },
		{ from: 6, color: USO_HEX[5], labelKey: 'uso.class.estero' },
		{ from: 7, color: USO_HEX[6], labelKey: 'uso.class.agua' },
		{ from: 8, color: USO_HEX[7], labelKey: 'uso.class.urbano' },
		{ from: 9, color: USO_HEX[8], labelKey: 'uso.class.mosaico' },
	],
};

// 1985 → 2025 over the same classes, named by where the cell ended; same colors as its destination class.
export const USO_TRAY_CLASSES: ColorClasses = {
	nominal: true,
	steps: [
		{ from: 1, color: USO_HEX[0], labelKey: 'uso.tray.monte' },
		{ from: 2, color: USO_HEX[1], labelKey: 'uso.tray.plantacion' },
		{ from: 3, color: USO_HEX[2], labelKey: 'uso.tray.perenne' },
		{ from: 4, color: USO_HEX[3], labelKey: 'uso.tray.agro' },
		{ from: 5, color: USO_HEX[6], labelKey: 'uso.tray.urbanoAgua' },
		{ from: 6, color: USO_HEX[5], labelKey: 'uso.tray.volvio' },
		{ from: 7, color: USO_HEX[4], labelKey: 'uso.tray.otro' },
		{ from: 8, color: USO_HEX[8], labelKey: 'uso.tray.sinMonte' },
	],
};

// % of the cell in one class (Plantación, Yerba y té, Pastura y cultivo tabs). 0 = measured none.
export const USO_PCT_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.uso.none' },
	steps: [
		{ from: 0, color: MAP_SELVA[1], labelKey: 'legend.uso.lt10' },
		{ from: 10, color: MAP_SELVA[2], labelKey: 'legend.uso.10_25' },
		{ from: 25, color: MAP_SELVA[3], labelKey: 'legend.uso.25_50' },
		{ from: 50, color: MAP_SELVA[4], labelKey: 'legend.uso.50_75' },
		{ from: 75, color: MAP_SELVA[5], labelKey: 'legend.uso.ge75' },
	],
};

// Forest plantations to harvest (pipeline/forestal/exportar_cosecha_web.py): age class of each DNDFI macizo
// (ordinal, selva ramp) and its situation against the rotation validated by the author on 3-nov-2026.
export const MACIZO_EDAD_CLASSES: ColorClasses = {
	ordinal: true,
	zero: { color: ZERO_HEX, labelKey: 'harvest.edad.sin' },
	steps: [
		{ from: 1, color: MAP_SELVA[0], labelKey: 'harvest.edad.1' },
		{ from: 2, color: MAP_SELVA[1], labelKey: 'harvest.edad.2' },
		{ from: 3, color: MAP_SELVA[2], labelKey: 'harvest.edad.3' },
		{ from: 4, color: MAP_SELVA[3], labelKey: 'harvest.edad.4' },
		{ from: 5, color: MAP_SELVA[4], labelKey: 'harvest.edad.5' },
		{ from: 6, color: MAP_SELVA[5], labelKey: 'harvest.edad.6' },
	],
};
// 1–4 run toward harvest (HARVEST_HEX: index 3 = «more» in both themes, bright on dark and dark on paper); 5 = Hansen
// loss after planting (a different hue); 6 = no planting year or no cited rotation (the map's «sin dato» gray).
export const MACIZO_SITUACION_CLASSES: ColorClasses = {
	nominal: true,
	steps: [
		{ from: 1, color: HARVEST_HEX[0], labelKey: 'harvest.sit.1' },
		{ from: 2, color: HARVEST_HEX[1], labelKey: 'harvest.sit.2' },
		{ from: 3, color: HARVEST_HEX[2], labelKey: 'harvest.sit.3' },
		{ from: 4, color: HARVEST_HEX[3], labelKey: 'harvest.sit.4' },
		{ from: 5, color: USO_HEX[6], labelKey: 'harvest.sit.5' },
		{ from: 6, color: NODATA_HEX, labelKey: 'harvest.sit.6' },
	],
};

// JRC occurrence (% of 1984–2021 with water). Cuts match the methodology text
// (<5 / 5–20 / >20). 0 % = Landsat never saw water there — a real reading, not a
// coverage gap: it was painted "Sin cobertura" (97 % of Eldorado) by the legacy
// value-0-as-nodata rule in Map.computeHexColor.
export const FLOOD_OCCURRENCE_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.flood.none' },
	steps: [
		{ from: 0, color: MAP_CLASS_3[0], labelKey: 'legend.flood.low' },
		{ from: 5, color: MAP_CLASS_3[1], labelKey: 'legend.flood.mid' },
		{ from: 20, color: MAP_CLASS_3[2], labelKey: 'legend.flood.high' },
	],
};

// Per-hex relief variables, painted when a flood ranking tab is active
// (deptRank.hexVariable). Same cuts in every territory. Cuts from the Misiones
// distribution of cells with any building on low ground (p50 2, p75 7, p90 16,
// p99 139) and of added buildings there 2016–2023 (p50 1, p75 3, p90 7, p99 41).
export const FLOOD_LOW_BLD_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.lowBld.none' },
	zeroBelow: 0.5,
	steps: [
		{ from: 0.5, color: MAP_CLASS_4[0], labelKey: 'legend.lowBld.c1' },
		{ from: 2.5, color: MAP_CLASS_4[1], labelKey: 'legend.lowBld.c2' },
		{ from: 9.5, color: MAP_CLASS_4[2], labelKey: 'legend.lowBld.c3' },
		{ from: 49.5, color: MAP_CLASS_4[3], labelKey: 'legend.lowBld.c4' },
	],
};
// Share of the cell's buildings on low ground (tab % edificios). 0 = none there,
// or no buildings at all (the pipeline writes 0, not null). Same cuts as % tierra.
export const FLOOD_LOW_BLD_PCT_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.lowBld.none' },
	steps: [
		{ from: 0, color: MAP_CLASS_3[0], labelKey: 'legend.lowLand.c1' },
		{ from: 25, color: MAP_CLASS_3[1], labelKey: 'legend.lowLand.c2' },
		{ from: 75, color: MAP_CLASS_3[2], labelKey: 'legend.lowLand.c3' },
	],
};
export const FLOOD_LOW_LAND_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.lowLand.none' },
	steps: [
		{ from: 0, color: MAP_CLASS_3[0], labelKey: 'legend.lowLand.c1' },
		{ from: 25, color: MAP_CLASS_3[1], labelKey: 'legend.lowLand.c2' },
		{ from: 75, color: MAP_CLASS_3[2], labelKey: 'legend.lowLand.c3' },
	],
};
export const FLOOD_LOW_NEW_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.lowNew.none' },
	zeroBelow: 0.5,
	steps: [
		{ from: -1e9, color: '#64748b', labelKey: 'legend.lowNew.down' },
		{ from: 0.5, color: MAP_CLASS_3[0], labelKey: 'legend.lowNew.c1' },
		{ from: 4.5, color: MAP_CLASS_3[1], labelKey: 'legend.lowNew.c2' },
		{ from: 19.5, color: MAP_CLASS_3[2], labelKey: 'legend.lowNew.c3' },
	],
};

// Hansen loss rate (%/yr, 2016–2025). Same cuts and colors (MAP_CLASS_3) as the layer's
// flowBands so map and flow chart classify a hexagon alike. 0 = no loss
// detected — in Eldorado half the hexagons, median tree cover 98 %: standing
// forest, previously painted "Sin cobertura".
export const DEFOREST_LOSS_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.deforest.none' },
	steps: [
		{ from: 0, color: MAP_CLASS_3[0], labelKey: 'legend.deforest.low' },
		{ from: 0.5, color: MAP_CLASS_3[1], labelKey: 'legend.deforest.mid' },
		{ from: 2, color: MAP_CLASS_3[2], labelKey: 'legend.deforest.high' },
	],
	upperInclusive: true,
};

// Travel time to the capital (Oxford MAP, minutes), doubling cuts (1/2/4/8 h), the
// same in every territory. Labels in minutes, like the tooltip and the panel: the
// legend takes the variable's unit (min). Medians run from 73 min (Central PY) to 403 (Alto
// Paraguay); Misiones 6 / 21 / 36 / 35 / 3 %. Since the 1-nov-2026 rebuild a 0 is
// a real reading (the capital's own cells, ~9 per territory): split_by_admin keeps
// *_raw columns NULL where there is no reading ("Sin cobertura"). Before, it wrote
// them as 0 (1,008 cells in Capital, 2,033 in Villeta).
export const ACCESS_TIME_CLASSES: ColorClasses = {
	steps: [
		{ from: 0, color: MAP_ACCESS_5[0], labelKey: 'legend.access.c1' },
		{ from: 60, color: MAP_ACCESS_5[1], labelKey: 'legend.access.c2' },
		{ from: 120, color: MAP_ACCESS_5[2], labelKey: 'legend.access.c3' },
		{ from: 240, color: MAP_ACCESS_5[3], labelKey: 'legend.access.c4' },
		{ from: 480, color: MAP_ACCESS_5[4], labelKey: 'legend.access.c5' },
	],
};

// Travel time by road to nearby destinations (hospital, health establishment,
// secondary school, trunk road), minutes. Since 5-nov-2026 every accessibility
// column is minutes by road (no straight-line km); these destinations are much
// closer than the capital, so they get their own cuts (15 / 30 / 60 / 120 min).
// economic_activity, tab Desocupación: desocupados / activos of the cell's radio
// (pipeline/add_econ_hex_derived.py). 0 = a radio with no unemployed, a measured value.
// Cuts over the NEA spread (P50 2–6 %, P90 6–13 %, P98 10–19 % by territory).
export const DESOCUP_CLASSES: ColorClasses = {
	zero: { color: ZERO_HEX, labelKey: 'legend.desocup.zero' },
	zeroBelow: 0.05,
	steps: [
		{ from: 0.05, color: MAP_CLASS_5[0], labelKey: 'legend.desocup.c1' },
		{ from: 3, color: MAP_CLASS_5[1], labelKey: 'legend.desocup.c2' },
		{ from: 6, color: MAP_CLASS_5[2], labelKey: 'legend.desocup.c3' },
		{ from: 10, color: MAP_CLASS_5[3], labelKey: 'legend.desocup.c4' },
		{ from: 15, color: MAP_CLASS_5[4], labelKey: 'legend.desocup.c5' },
	],
};
// economic_activity, tab Ocupados: the radio's employed spread over the cell's estimated
// people 14+ (fractional: half the populated cells hold less than one). P98 98–142, max ~900.
export const OCUP_HEX_CLASSES: ColorClasses = {
	steps: [
		{ from: 0, color: MAP_CLASS_5[0], labelKey: 'legend.ocupHex.c1' },
		{ from: 1, color: MAP_CLASS_5[1], labelKey: 'legend.ocupHex.c2' },
		{ from: 5, color: MAP_CLASS_5[2], labelKey: 'legend.ocupHex.c3' },
		{ from: 20, color: MAP_CLASS_5[3], labelKey: 'legend.ocupHex.c4' },
		{ from: 100, color: MAP_CLASS_5[4], labelKey: 'legend.ocupHex.c5' },
	],
};

export const ACCESS_NEAR_CLASSES: ColorClasses = {
	steps: [
		{ from: 0, color: MAP_ACCESS_5[0], labelKey: 'legend.accessNear.c1' },
		{ from: 15, color: MAP_ACCESS_5[1], labelKey: 'legend.accessNear.c2' },
		{ from: 30, color: MAP_ACCESS_5[2], labelKey: 'legend.accessNear.c3' },
		{ from: 60, color: MAP_ACCESS_5[3], labelKey: 'legend.accessNear.c4' },
		{ from: 120, color: MAP_ACCESS_5[4], labelKey: 'legend.accessNear.c5' },
	],
};

export interface ScaleStats {
	generated: string;
	bands: number[];
	territories: Record<string, {
		label: string; country: string; n: number;
		min: number; p2: number; p98: number; max: number;
		bandsPct: number[];   // % of hexagons per band: ≤bands[0], …, >bands[last]
	}>;
}

// Generated by pipeline/build_pm25_scale_stats.py from the served parquets.
export const PM25_SCALE: ScaleStats = pm25Scale;

export interface FlowBands {
	col: string;          // current-period column in visibleData
	baselineCol: string;  // baseline-period column
	breaks: number[];     // ascending inner cut points (labels.length - 1)
	labels: string[];     // band labels, low value -> high value
	colors: string[];     // one per label
	higherIsWorse?: boolean; // flips improved/worsened semantics in flows
	note?: string;        // footnote describing the cuts + source
	// Direction words for the summary line. Defaults: mejoraron/empeoraron.
	// Use neutral terms when the variable is not normative (e.g. density).
	// Physical unit of col/baselineCol. When set, the chart adds a magnitude line
	// (mean baseline → current, change, share of hexagons that went down/up):
	// bands of a few units hide uniform changes that never cross a cut.
	unit?: string;
	upLabel?: string;     // moved toward the "better" end (per higherIsWorse)
	downLabel?: string;
}

/**
 * Physical-unit label for a hex layer's primary variable, for tooltips/axes.
 * Returns the declared unit (tC/ha, min, %, µg/m³, mm…), 'percentil prov.' for
 * census layers, or '/100' only for genuine 0-100 composite scores. Matches the
 * primary against both `col` and `rawCol` (carbon's primary is a rawCol whose unit
 * lives on the base entry). Mirrors primaryXLabel in Sidebar.svelte.
 */
export function getHexPrimaryUnit(layer: HexLayerConfig | null | undefined, col?: string): string {
	if (!layer) return '';
	const pv = col ?? layer.primaryVariable;
	const v = layer.variables?.find((x) => x.col === pv || x.rawCol === pv);
	if (v?.unit && v.unit !== '/100' && v.unit !== 'percentil') return unitLabel(v.unit);
	if (layer.variables?.some((x) => x.unit === 'percentil')) return i18n.t('unit.pctlProv');
	// Genuine 0-100 composite score (eudr risk_score, forestry score) → '/100'.
	// Anything else with no declared unit (e.g. a raw 0-1 fraction) → no suffix,
	// so we never re-introduce a fake '/100' on a raw value.
	if (pv === 'score' || pv.endsWith('_score') || v?.unit === '/100') return '/100';
	return '';
}

/**
 * Chart axis title for a hex layer variable: what it measures plus its unit,
 * e.g. "Carbono en biomasa (tC/ha)". Falls back to the layer title when the
 * variable is not declared (substituted primaries, delta/baseline columns).
 */
/**
 * Whether a hex variable can be summed across cells (counts, or per-area
 * densities on equal-area hexagons): the concentration curve only makes sense
 * then. Shares of households, minutes, µg/m³ or /100 scores do not add up.
 */
const ADDITIVE_UNITS = new Set(['edificios', 'personas', 'hab/km²', 'viv/km²', 'edif/km²', 'ha', 'ha/año', '%/año', '%/yr', 'nW/cm²/sr', 'tC/ha']);
export function isAdditiveVar(layer: HexLayerConfig | null | undefined, col?: string): boolean {
	if (!layer) return false;
	const pv = col ?? layer.primaryVariable;
	const v = layer.variables?.find((x) => x.col === pv || x.rawCol === pv);
	if (!v) return false;
	if (v.aggregation === 'sum') return true;
	const unit = v.rawCol === pv ? (v.rawUnit ?? v.unit) : v.unit;
	return !!unit && ADDITIVE_UNITS.has(unit);
}

export function getHexAxisLabel(layer: HexLayerConfig | null | undefined, col?: string, withUnit = true): string {
	if (!layer) return '';
	const pv = col ?? layer.primaryVariable;
	const v = layer.variables?.find((x) => x.col === pv || x.rawCol === pv);
	const name = v?.labelKey ? i18n.t(v.labelKey) : i18n.t(layer.titleKey);
	const unit = withUnit ? getHexPrimaryUnit(layer, pv) : '';
	// Some labels already carry their unit ("Tasa de pérdida (%/año)").
	return unit && !name.includes(unit) ? `${name} (${unit})` : name;
}

export function getTemporalCol(col: string, mode: TemporalMode): string {
	if (mode === 'current') return col;
	if (mode === 'baseline') return col === 'score' ? 'score_baseline' : `${col}_baseline`;
	return col === 'score' ? 'delta_score' : `${col}_delta`;
}

export const HEX_LAYER_REGISTRY: Record<string, HexLayerConfig> = {
	flood_risk: {
		id: 'flood_risk',
		parquet: 'hex_flood_risk',
		variables: [
			{ col: 'jrc_occurrence', labelKey: 'analysis.flood.jrcOccurrence', aggregation: 'mean', unit: '%' },
			{ col: 'jrc_recurrence', labelKey: 'analysis.flood.jrcRecurrence', aggregation: 'mean', unit: '%' },
			{ col: 'jrc_seasonality', labelKey: 'analysis.flood.jrcSeasonality', aggregation: 'mean', unit: 'meses' },
			{ col: 'flood_extent_pct', labelKey: 'analysis.flood.currentExtent', aggregation: 'mean', unit: '%' },
			// Relief (build_flood_rank.py --hex): painted from the ranking tabs.
			{ col: 'low_bld_pct', labelKey: 'analysis.flood.lowBldPct', aggregation: 'mean', unit: '%' },
			{ col: 'low_bld_n', labelKey: 'analysis.flood.lowBld', aggregation: 'sum', unit: 'edificios' },
			{ col: 'low_land_pct', labelKey: 'analysis.flood.lowLand', aggregation: 'mean', unit: '%' },
			{ col: 'low_bld_new_n', labelKey: 'analysis.flood.lowBldNew', aggregation: 'sum', unit: 'edificios' },
		],
		primaryVariable: 'jrc_occurrence',
		legendLowKey: 'legend.flood.lowExp',
		legendHighKey: 'legend.flood.highExp',
		colorScale: 'flood',
		aggregation: 'mean',
		petalVars: [
			{ col: 'jrc_occurrence', labelKey: 'analysis.flood.jrcOccurrence', aggregation: 'mean' },
			{ col: 'jrc_recurrence', labelKey: 'analysis.flood.jrcRecurrence', aggregation: 'mean' },
			{ col: 'jrc_seasonality', labelKey: 'analysis.flood.jrcSeasonality', aggregation: 'mean' },
			{ col: 'flood_extent_pct', labelKey: 'analysis.flood.currentExtent', aggregation: 'mean' },
		],
		titleKey: 'analysis.floodRisk.title',
		perDepartment: true,
		colorClasses: FLOOD_OCCURRENCE_CLASSES,
		// With a ranking tab active the hexes paint its relief variable, so a
		// department reads like its bar; JRC stays the color on the saturated plain.
		variableClasses: { low_bld_pct: FLOOD_LOW_BLD_PCT_CLASSES, low_bld_n: FLOOD_LOW_BLD_CLASSES, low_land_pct: FLOOD_LOW_LAND_CLASSES, low_bld_new_n: FLOOD_LOW_NEW_CLASSES },
		// Exposure by terrain (HAND < 5 m), not JRC: JRC occurrence ranks river and
		// reservoir shores (Capital on Yacyretá, Iguazú on Urugua-í) and misses the
		// short floods of the plateau. pipeline/build_flood_rank.py.
		deptRank: {
			field: 'bld_hand5_pct', unit: '%', decimals: 1,
			titleKey: 'rank.flood.title', noteKey: 'rank.flood.note',
			extraField: 'land_hand5_pct', extraUnitKey: 'rank.unit.land', extraDecimals: 1,
			saturatedKey: 'rank.flood.saturated',
			// Each tab paints its own reading: % of the cell's buildings / buildings.
			hexVariable: 'low_bld_pct',
			// Three readings of the same HAND export: share of buildings (default),
			// number of buildings (magnitude, follows population), share of land (rural).
			metrics: [
				{ tabKey: 'rank.flood.tab.pct' },
				{ tabKey: 'rank.flood.tab.n', field: 'bld_hand5_n', unit: '', unitKey: 'rank.unit.buildings', decimals: 0, hexVariable: 'low_bld_n',
				  titleKey: 'rank.flood.title.n', noteKey: 'rank.flood.note.n',
				  extraField: 'bld_total', extraUnitKey: 'rank.unit.bldTotal', extraDecimals: 0 },
				{ tabKey: 'rank.flood.tab.land', field: 'land_hand5_pct', unit: '%', unitKey: 'rank.unit.pct', decimals: 1, hexVariable: 'low_land_pct',
				  titleKey: 'rank.flood.title.land', noteKey: 'rank.flood.note.land',
				  extraField: 'bld_hand5_pct', extraUnitKey: 'rank.unit.bldPct', extraDecimals: 1 },
				// Has new construction drifted to low ground? Open Buildings Temporal 2016–2023,
				// only where the department order is stable (build_flood_rank.py --change).
				{ tabKey: 'rank.flood.tab.change', field: 'low_tilt_pts', unit: 'pts', unitKey: 'rank.unit.pts', decimals: 1, signed: true, hexVariable: 'low_bld_new_n',
				  titleKey: 'rank.flood.title.change', noteKey: 'rank.flood.note.change',
				  extraField: 'low_new_n', extraUnitKey: 'rank.unit.newLow', extraDecimals: 0, province: 'change' },
			],
		},
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	// ── Satellite composite scores ──
	agri_potential: {
		id: 'agri_potential',
		// Empirical version (1-nov-2026, pipeline/hexoma/exportar_agri_web.py): physical units and
		// crop suitability from the Hexoma, no scores. NEA AR only. The ranking tabs pick the
		// crop the map paints (same mechanism as the flood tabs).
		deptRank: {
			field: 'soja_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
			titleKey: 'rank.agri.title', noteKey: 'rank.agri.note',
			extraField: 'soja_pct', extraUnitKey: 'rank.unit.pctArea', extraDecimals: 1,
			hexVariable: 'apt_soja_clase',
			metrics: [
				{ tabKey: 'rank.agri.tab.soja' },
				{ tabKey: 'rank.agri.tab.maiz', field: 'maiz_ha', extraField: 'maiz_pct', hexVariable: 'apt_maiz_clase' },
				{ tabKey: 'rank.agri.tab.algodon', field: 'algodon_ha', extraField: 'algodon_pct', hexVariable: 'apt_algodon_clase' },
				{ tabKey: 'rank.agri.tab.girasol', field: 'girasol_ha', extraField: 'girasol_pct', hexVariable: 'apt_girasol_clase' },
				{ tabKey: 'rank.agri.tab.arroz', field: 'arroz_ha', extraField: 'arroz_pct', hexVariable: 'apt_arroz_clase' },
				{ tabKey: 'rank.agri.tab.yerba', field: 'yerba_ha', extraField: 'yerba_pct', hexVariable: 'apt_yerba_clase' },
				{ tabKey: 'rank.agri.tab.te', field: 'te_ha', extraField: 'te_pct', hexVariable: 'apt_te_clase' },
				{ tabKey: 'rank.agri.tab.citricos', field: 'citricos_ha', extraField: 'citricos_pct', hexVariable: 'apt_citricos_clase' },
				{ tabKey: 'rank.agri.tab.pasturas', field: 'pasturas_ha', extraField: 'pasturas_pct', hexVariable: 'apt_pasturas_clase' },
			],
		},
		parquet: 'sat_agri_apt',
		variables: [
			{ col: 'apt_soja_pct', labelKey: 'sat.agri.apt.soja', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_maiz_pct', labelKey: 'sat.agri.apt.maiz', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_algodon_pct', labelKey: 'sat.agri.apt.algodon', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_girasol_pct', labelKey: 'sat.agri.apt.girasol', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_arroz_pct', labelKey: 'sat.agri.apt.arroz', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_yerba_pct', labelKey: 'sat.agri.apt.yerba', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_te_pct', labelKey: 'sat.agri.apt.te', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_citricos_pct', labelKey: 'sat.agri.apt.citricos', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'apt_pasturas_pct', labelKey: 'sat.agri.apt.pasturas', aggregation: 'mean', unit: '% campañas', noPetal: true },
			{ col: 'p_normal_mm', labelKey: 'sat.agri.pNormal', aggregation: 'mean', unit: 'mm' },
			{ col: 'balance_normal_mm', labelKey: 'sat.agri.balance', aggregation: 'mean', unit: 'mm' },
			{ col: 'gdd_normal', labelKey: 'sat.agri.gddNormal', aggregation: 'mean', unit: '°C·d' },
			{ col: 'heladas_normal', labelKey: 'sat.agri.heladas', aggregation: 'mean', unit: 'días/año' },
			{ col: 'heladas5_normal', labelKey: 'sat.agri.heladas5', aggregation: 'mean', unit: 'días/año' },
			{ col: 'calor35_normal', labelKey: 'sat.agri.calor35', aggregation: 'mean', unit: 'días/año' },
			{ col: 'ph', labelKey: 'sat.agri.phSuelo', aggregation: 'mean', unit: 'pH' },
			{ col: 'arcilla_pct', labelKey: 'sat.agri.arcilla', aggregation: 'mean', unit: '%' },
			{ col: 'pendiente_pct', labelKey: 'sat.agri.pendiente', aggregation: 'mean', unit: '%' },
			{ col: 'elev_m', labelKey: 'sat.agri.elev', aggregation: 'mean', unit: 'm' },
			{ col: 'apt_soja_clase', labelKey: 'sat.agri.clase.soja', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_maiz_clase', labelKey: 'sat.agri.clase.maiz', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_algodon_clase', labelKey: 'sat.agri.clase.algodon', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_girasol_clase', labelKey: 'sat.agri.clase.girasol', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_arroz_clase', labelKey: 'sat.agri.clase.arroz', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_yerba_clase', labelKey: 'sat.agri.clase.yerba', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_te_clase', labelKey: 'sat.agri.clase.te', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_citricos_clase', labelKey: 'sat.agri.clase.citricos', aggregation: 'mean', unit: 'S1–N', hidden: true },
			{ col: 'apt_pasturas_clase', labelKey: 'sat.agri.clase.pasturas', aggregation: 'mean', unit: 'S1–N', hidden: true },
		],
		petalVars: [
			{ col: 'p_normal_mm', labelKey: 'sat.agri.pNormal', aggregation: 'mean' },
			{ col: 'gdd_normal', labelKey: 'sat.agri.gddNormal', aggregation: 'mean' },
			{ col: 'heladas5_normal', labelKey: 'sat.agri.heladas5', aggregation: 'mean' },
			{ col: 'ph', labelKey: 'sat.agri.phSuelo', aggregation: 'mean' },
			{ col: 'arcilla_pct', labelKey: 'sat.agri.arcilla', aggregation: 'mean' },
			{ col: 'pendiente_pct', labelKey: 'sat.agri.pendiente', aggregation: 'mean' },
		],
		primaryVariable: 'apt_soja_clase',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.agri.title',
		legendHighKey: 'legend.agri.highN',
		perDepartment: true,
		colorClasses: AGRI_APT_CLASSES,
		variableClasses: { apt_soja_clase: AGRI_APT_CLASSES, apt_maiz_clase: AGRI_APT_CLASSES, apt_algodon_clase: AGRI_APT_CLASSES, apt_girasol_clase: AGRI_APT_CLASSES, apt_arroz_clase: AGRI_APT_CLASSES, apt_yerba_clase: AGRI_APT_CLASSES, apt_te_clase: AGRI_APT_CLASSES, apt_citricos_clase: AGRI_APT_CLASSES, apt_pasturas_clase: AGRI_APT_CLASSES },
		coverage: { corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'unavailable', itapua_py: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable' },
	},
	forestry_aptitude: {
		id: 'forestry_aptitude',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'high_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
			titleKey: 'rank.forestry.title', noteKey: 'rank.forestry.note',
			extraField: 'high_pct', extraUnitKey: 'rank.unit.pctArea', extraDecimals: 1,
		},
		parquet: 'sat_forestry_aptitude',
		variables: [
			{ col: 'type_label', labelKey: 'sat.forestry.typeLabel', aggregation: 'mean' },
			{ col: 'gdd', labelKey: 'sat.forestry.gdd', aggregation: 'mean', unit: '°C·d' },
			{ col: 'precip_total', labelKey: 'sat.forestry.precip', aggregation: 'mean', unit: 'mm' },
			{ col: 'water_deficit', labelKey: 'sat.forestry.waterDeficit', aggregation: 'mean', unit: 'mm' },
			{ col: 'slope_mean', labelKey: 'sat.forestry.slope', aggregation: 'mean', unit: '°' },
			{ col: 'clay', labelKey: 'sat.forestry.clay', aggregation: 'mean', unit: '%' },
			{ col: 'soc', labelKey: 'sat.forestry.soc', aggregation: 'mean', unit: 'g/kg' },
			{ col: 'score', labelKey: 'sat.forestry.score', aggregation: 'mean', unit: '/100' },
		],
		primaryVariable: 'score',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.forestry.title',
		perDepartment: true,
		legendLowKey: 'legend.forestry.low',
		legendHighKey: 'legend.forestry.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	service_deprivation: {
		id: 'service_deprivation',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'nbi_pct', unit: '%', decimals: 1, hexVariable: 'c_nbi',
			titleKey: 'rank.service.title', noteKey: 'rank.service.note',
			extraField: 'nbi_n', extraUnitKey: 'rank.unit.hogares', extraDecimals: 0,
			metrics: [
				{ tabKey: 'rank.service.tab.nbi' },
				{ tabKey: 'rank.service.tab.cloacas', field: 'cloacas_pct', extraField: 'cloacas_n', hexVariable: 'c_sin_cloacas' },
				{ tabKey: 'rank.service.tab.hacin', field: 'hacin_pct', extraField: 'hacin_n', hexVariable: 'c_hacinamiento' },
			],
		},
		parquet: 'sat_service_deprivation',
		variables: [
			// Real Census 2022 rates, % of households (pipeline/build_census_rates.py).
			// No K-means classification for this layer → no "Clasificación" row.
			{ col: 'c_nbi', labelKey: 'sat.deprivation.nbi', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_sin_cloacas', labelKey: 'sat.deprivation.sinCloacas', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_piso', labelKey: 'sat.deprivation.piso', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_hacinamiento', labelKey: 'sat.deprivation.hacinamiento', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_hacinamiento_crit', labelKey: 'sat.deprivation.hacinamientoCrit', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_sin_computadora', labelKey: 'sat.deprivation.sinComputadora', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
		],
		quantileDomain: true,
		primaryVariable: 'c_nbi',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.deprivation.title',
		perDepartment: true,
		legendLowKey: 'legend.deprivation.low',
		legendHighKey: 'legend.deprivation.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	health_access: {
		id: 'health_access',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'health_min', unit: 'min', decimals: 0,
			titleKey: 'rank.health.title', noteKey: 'rank.health.note',
			extraField: 'health_cov_pct', extraUnitKey: 'rank.unit.pctCoverage', extraDecimals: 1,
		},
		parquet: 'sat_health_access',
		variables: [
			// Oxford/MAP minutes and real Census 2022 rates (pipeline/build_census_rates.py).
			{ col: 'c_healthcare_time', labelKey: 'sat.health.time', aggregation: 'mean', unit: 'min' },
			{ col: 'c_health_coverage', labelKey: 'sat.health.coverage', aggregation: 'mean', unit: '%', baseCol: 'pob_total' },
			{ col: 'c_nbi', labelKey: 'sat.health.nbi', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'c_elderly', labelKey: 'sat.health.elderly', aggregation: 'mean', unit: '%', baseCol: 'pob_total' },
			{ col: 'c_children', labelKey: 'sat.health.children', aggregation: 'mean', unit: '%', baseCol: 'pob_total' },
			{ col: 'c_pop_density', labelKey: 'sat.health.popDensity', aggregation: 'mean', unit: 'hab/km²' },
		],
		quantileDomain: true,
		primaryVariable: 'c_healthcare_time',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.health.title',
		perDepartment: true,
		legendLowKey: 'legend.health.low',
		legendHighKey: 'legend.health.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	education_capital: {
		id: 'education_capital',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'noschool_pct', unit: '%', decimals: 1, hexVariable: 'c_no_schooling',
			extraField: 'noschool_cambio_pts', extraUnitKey: 'rank.educap.unit.noschoolCambio', extraDecimals: 1,
			titleKey: 'rank.educap.title', noteKey: 'rank.educap.note',
			metrics: [
				{ tabKey: 'rank.educap.tab.noschool' },
				{ tabKey: 'rank.educap.tab.univ', field: 'univ25_pct', noteKey: 'rank.econ.note.univ', hexVariable: 'c_university',   // 25+: pipeline/build_univ25_rank.py
				  extraField: 'univ_cambio_pts', extraUnitKey: 'rank.econ.unit.univCambio' },
			],
		},
		parquet: 'sat_education_capital',
		variables: [
			// Real Census 2022 rates, % of people aged 18+ (pipeline/build_census_rates.py).
			{ col: 'c_no_schooling', labelKey: 'sat.eduCap.noSchooling', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_secondary_plus', labelKey: 'sat.eduCap.secondaryPlus', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_higher_edu', labelKey: 'sat.eduCap.higherEdu', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_university', labelKey: 'sat.eduCap.university', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			// Censo 2010 with the 2022 apportionment, for the transition charts (pipeline/build_census_trayectorias.py).
			{ col: 'c_no_schooling_2010', labelKey: 'sat.eduCap.noSchooling', aggregation: 'mean', unit: '%', hidden: true },
			{ col: 'c_university_2010', labelKey: 'sat.eduCap.university', aggregation: 'mean', unit: '%', hidden: true },
		],
		tabFlows: {
			c_no_schooling: {
				periods: { baseline: '2010', current: '2022' },
				bands: {
					col: 'c_no_schooling', baselineCol: 'c_no_schooling_2010', breaks: [15, 30, 45],
					labels: ['< 15 %', '15–30 %', '30–45 %', '≥ 45 %'],
					colors: [MAP_CLASS_4[3], MAP_CLASS_4[2], MAP_CLASS_4[1], MAP_CLASS_4[0]],
					higherIsWorse: true, unit: '%', note: 'flow.noschool.note',
				},
			},
			c_university: {
				periods: { baseline: '2010', current: '2022' },
				bands: {
					col: 'c_university', baselineCol: 'c_university_2010', breaks: [2, 5, 10],
					labels: ['flow.univ.b1', 'flow.univ.b2', 'flow.univ.b3', 'flow.univ.b4'],
					colors: [MAP_SELVA[1], MAP_SELVA[2], MAP_SELVA[4], MAP_SELVA[5]],
					higherIsWorse: false, unit: '%', upLabel: 'flow.univ.up', downLabel: 'flow.univ.down', note: 'flow.univ.note',
				},
			},
		},
		quantileDomain: true,
		primaryVariable: 'c_no_schooling',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.eduCap.title',
		perDepartment: true,
		legendLowKey: 'legend.eduCap.low',
		legendHighKey: 'legend.eduCap.high',
		variableLegend: { c_university: { low: 'legend.univ.low', high: 'legend.univ.high' } },
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	education_flow: {
		id: 'education_flow',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'dropout_sec_pct', unit: '%', decimals: 1, hexVariable: 'c_dropout_secondary',
			titleKey: 'rank.eduflow.title', noteKey: 'rank.eduflow.note',
			metrics: [
				{ tabKey: 'rank.eduflow.tab.dropout' },
				{ tabKey: 'rank.eduflow.tab.teen', field: 'teen_preg_pct', hexNone: true },
			],
		},
		parquet: 'sat_education_flow',
		variables: [
			// Real Census 2022 rates (pipeline/build_census_rates.py). Teen motherhood
			// (c_teen_pregnancy) stays in the parquet for the department ranking only:
			// its base, girls 14–17, has a median of 19 per radio.
			{ col: 'c_dropout_primary', labelKey: 'sat.eduFlow.dropoutPrimary', aggregation: 'mean', unit: '%', baseCol: 'pob_6_12' },
			{ col: 'c_dropout_secondary', labelKey: 'sat.eduFlow.dropoutSecondary', aggregation: 'mean', unit: '%', baseCol: 'pob_13_18' },
		],
		quantileDomain: true,
		primaryVariable: 'c_dropout_primary',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.eduFlow.title',
		perDepartment: true,
		legendLowKey: 'legend.eduFlow.low',
		legendHighKey: 'legend.eduFlow.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	// ── Land use / MapBiomas ──
	land_use: {
		id: 'land_use',
		// MapBiomas Argentina Colección 3 from the Hexoma (pipeline/hexoma/exportar_uso_web.py, 3-nov-2026), NEA AR
		// only. Replaces Dynamic World 2024, whose «trees» lumped monte, pine and much of yerba/tea in Misiones.
		// The map paints a class (≥ 50 % of the cell, else mosaico); the tabs repaint (deptRank.hexVariable).
		// Rankings and the 1985–2025 series come from the summaries, written by the same script.
		deptRank: {
			field: 'monte_pct', unit: '%', unitKey: 'rank.unit.pctArea', decimals: 1, hexVariable: 'dom_2025',
			titleKey: 'rank.uso.title.monte', noteKey: 'rank.uso.note.monte',
			extraField: 'monte_ha', extraUnitKey: 'rank.unit.ha', extraDecimals: 0,
			metrics: [
				{ tabKey: 'rank.uso.tab.uso2025' },
				{ tabKey: 'rank.uso.tab.plantacion', field: 'plantacion_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
				  extraField: 'plantacion_pct', extraUnitKey: 'rank.unit.pctArea', extraDecimals: 1,
				  titleKey: 'rank.uso.title.plantacion', noteKey: 'rank.uso.note.plantacion', hexVariable: 'pct_plantacion' },
				{ tabKey: 'rank.uso.tab.perenne', field: 'perenne_pct', extraField: undefined,
				  titleKey: 'rank.uso.title.perenne', noteKey: 'rank.uso.note.perenne', hexVariable: 'pct_perenne' },
				{ tabKey: 'rank.uso.tab.agro', field: 'agro_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
				  extraField: 'agro_pct', extraUnitKey: 'rank.unit.pctArea', extraDecimals: 1,
				  titleKey: 'rank.uso.title.agro', noteKey: 'rank.uso.note.agro', hexVariable: 'pct_agro' },
				{ tabKey: 'rank.uso.tab.uso1985', field: 'monte85_pct', extraField: 'monte85_ha',
				  titleKey: 'rank.uso.title.monte85', noteKey: 'rank.uso.note.monte85', hexVariable: 'dom_1985' },
				{ tabKey: 'rank.uso.tab.cambio', field: 'monte_perdido_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0, signed: true,
				  extraField: 'monte_perdido_pct', extraUnitKey: 'rank.uso.unit.pctMonte85', extraDecimals: 1,
				  titleKey: 'rank.uso.title.cambio', noteKey: 'rank.uso.note.cambio', hexVariable: 'tray' },
			],
		},
		parquet: 'sat_uso_suelo',
		// % of the cell's observed area in 2025 per MapBiomas group (panel); class codes hidden (map only).
		variables: [
			{ col: 'pct_monte',      labelKey: 'uso.var.monte',      aggregation: 'mean', unit: '%' },
			{ col: 'pct_plantacion', labelKey: 'uso.var.plantacion', aggregation: 'mean', unit: '%' },
			{ col: 'pct_perenne',    labelKey: 'uso.var.perenne',    aggregation: 'mean', unit: '%' },
			{ col: 'pct_cultivo',    labelKey: 'uso.var.cultivo',    aggregation: 'mean', unit: '%' },
			{ col: 'pct_pastura',    labelKey: 'uso.var.pastura',    aggregation: 'mean', unit: '%' },
			{ col: 'pct_pastizal',   labelKey: 'uso.var.pastizal',   aggregation: 'mean', unit: '%' },
			{ col: 'pct_estero',     labelKey: 'uso.var.estero',     aggregation: 'mean', unit: '%' },
			{ col: 'pct_urbano',     labelKey: 'uso.var.urbano',     aggregation: 'mean', unit: '%' },
			{ col: 'pct_agua',       labelKey: 'uso.var.agua',       aggregation: 'mean', unit: '%' },
			{ col: 'pct_otro',       labelKey: 'uso.var.otro',       aggregation: 'mean', unit: '%' },
			{ col: 'pct_agro',       labelKey: 'uso.var.agro',       aggregation: 'mean', unit: '%', hidden: true },
			{ col: 'dom_2025', labelKey: 'uso.var.dom2025', aggregation: 'mean', unit: 'clase', hidden: true },
			{ col: 'dom_1985', labelKey: 'uso.var.dom1985', aggregation: 'mean', unit: 'clase', hidden: true },
			{ col: 'tray',     labelKey: 'uso.var.tray',    aggregation: 'mean', unit: 'clase', hidden: true },
		],
		primaryVariable: 'dom_2025',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.landUse.title',
		perDepartment: true,
		colorClasses: USO_CLASSES,
		variableClasses: {
			dom_2025: USO_CLASSES, dom_1985: USO_CLASSES, tray: USO_TRAY_CLASSES,
			pct_plantacion: USO_PCT_CLASSES, pct_perenne: USO_PCT_CLASSES, pct_agro: USO_PCT_CLASSES,
		},
		// Per-department 1985–2025 series (ha by class every 5 years) in the summaries: UsoSerie.svelte.
		classSeries: true,
		coverage: { misiones: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'unavailable', itapua_py: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', cordillera_py: 'unavailable', san_pedro_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable' },
	},
	// ── Migrated from radio to H3 ──
	sociodemographic: {
		id: 'sociodemographic',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'nbi_pct', unit: '%', decimals: 1,
			titleKey: 'rank.socio.title', noteKey: 'rank.socio.note',
		},
		parquet: 'sat_sociodemographic',
		variables: [
			{ col: 'type', labelKey: 'analysis.socio.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'analysis.socio.typeLabel', aggregation: 'mean' },
			// Real Census 2022 values (pipeline/build_census_rates.py).
			{ col: 'densidad_hab_km2',   labelKey: 'radio.densidad',    aggregation: 'mean', unit: 'hab/km²' },
			{ col: 'pct_nbi',            labelKey: 'radio.nbi',          aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'pct_hacinamiento',   labelKey: 'radio.hacinamiento', aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'pct_propietario',    labelKey: 'radio.propietario',  aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
			{ col: 'tamano_medio_hogar', labelKey: 'radio.tamHogar',     aggregation: 'mean', unit: 'personas' },
			{ col: 'pct_computadora',    labelKey: 'radio.computadora',  aggregation: 'mean', unit: '%', baseCol: 'hog_total' },
		],
		quantileDomain: true,
		primaryVariable: 'pct_nbi',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'analysis.socio.title',
		perDepartment: true,
		legendLowKey: 'legend.socio.low',
		legendHighKey: 'legend.socio.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	// ── Cambio demográfico censal (INDEC 1991·2001·2010·2022, AR-only) ──
	// New, fully isolated layer: reuses the satellite temporal toggle for the
	// 2022↔2010 density delta; the full 4-census per-hexagon trajectory is shown
	// by CensoTemporalPanel (its own click-query), so `variables` is limited to the
	// temporal-safe pob_dens/viv_dens (which carry _baseline/_delta) — this avoids
	// touching the shared loadBaseResolution path. Areal apportionment to a fixed
	// populated H3 universe, renormalized to reconcile to INDEC totals.
	censo_temporal: {
		id: 'censo_temporal',
		parquet: 'sat_censo_temporal',
		variables: [
			{ col: 'pob_dens', labelKey: 'censo.pobDens', aggregation: 'mean', unit: 'hab/km²' },
			{ col: 'viv_dens', labelKey: 'censo.vivDens', aggregation: 'mean', unit: 'viv/km²' },
		],
		primaryVariable: 'pob_dens',
		colorScale: 'sequential',
		aggregation: 'mean',
		titleKey: 'analysis.censo.title',
		perDepartment: false,
		temporal: true,
		temporalPeriods: { current: '2022', baseline: '2010', source: 'INDEC · Rodríguez & de Grande (CONICET)' },
		// Density bands per Degree of Urbanisation (DEGURBA, UN Statistical
		// Commission 2020): rural <300, urban cluster 300-1500, urban centre
		// >=1500 hab/km². Density change is not normative -> neutral words.
		flowBands: {
			col: 'pob_dens',
			baselineCol: 'pob_dens_baseline',
			breaks: [300, 1500],
			labels: ['flow.censo.rural', 'flow.censo.cluster', 'flow.censo.center'],
			colors: [...MAP_VIRIDIS_3],   // = the map's viridis density ramp
			higherIsWorse: false,
			upLabel: 'flow.censo.up',
			downLabel: 'flow.censo.down',
			note: 'flow.censo.note',
		},
		legendLowKey: 'legend.censo.low',
		legendHighKey: 'legend.censo.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	economic_activity: {
		id: 'economic_activity',
		// Department ranking — pipeline/build_layer_ranks.py: Σ rate·base / Σ base over
		// the department's hexagons (same INDEC definition in the 4 provinces).
		// Ocupados (cantidad + % de la provincia) y desocupación: pipeline/build_empleo_rank.py, conteos por radio.
		// Por hexágono (pipeline/add_econ_hex_derived.py, 5-nov-2026): ocupados_est = los ocupados del radio
		// repartidos por la población 14+ estimada de la celda; tasa_desocupacion = desocupados / activos del
		// radio. Universitarios 25+: pipeline/build_univ25_rank.py.
		deptRank: {
			field: 'ocupados_n', unit: 'personas', unitKey: 'rank.unit.people', decimals: 0, hexVariable: 'ocupados_est',
			extraField: 'ocupados_prov_pct', extraUnitKey: 'rank.econ.unit.pctProv', extraDecimals: 1,
			titleKey: 'rank.econ.title', noteKey: 'rank.econ.note.ocupados',
			metrics: [
				{ tabKey: 'rank.econ.tab.ocupados' },
				{ tabKey: 'rank.econ.tab.empleo', titleKey: 'rank.econ.title.empleo', field: 'empleo_pct', unit: '%', unitKey: undefined, decimals: 1,
				  extraField: undefined, noteKey: 'rank.econ.note', hexVariable: 'tasa_empleo' },
				{ tabKey: 'rank.econ.tab.desocup', titleKey: 'rank.econ.title.desocup', field: 'desocup_pct', unit: '%', unitKey: undefined, decimals: 1, hexVariable: 'tasa_desocupacion',
				  extraField: undefined, noteKey: 'rank.econ.note.desocup' },
				{ tabKey: 'rank.econ.tab.univ', titleKey: 'rank.econ.title.univ', field: 'univ25_pct', unit: '%', unitKey: undefined, decimals: 1,
				  extraField: 'univ_cambio_pts', extraUnitKey: 'rank.econ.unit.univCambio', extraDecimals: 1,
				  noteKey: 'rank.econ.note.univ', hexVariable: 'pct_universitario' },
				{ tabKey: 'rank.econ.tab.viirs', titleKey: 'rank.econ.title.viirs', field: 'viirs', unit: 'nW/cm²/sr', unitKey: undefined, decimals: 2,
				  extraField: 'viirs_cambio_pct', extraUnitKey: 'rank.econ.unit.viirsCambio', extraDecimals: 0,
				  noteKey: 'rank.econ.note.viirs', hexVariable: 'viirs_mean_radiance' },
			],
		},
		parquet: 'sat_economic_activity',
		variables: [
			// Real Census 2022 rates (pipeline/build_econ_census_rates.py); baseCol = the
			// estimated people each one is over, shown in the hex panel's table.
			{ col: 'tasa_empleo', labelKey: 'radio.empleo', aggregation: 'mean', unit: '%', baseCol: 'pob_14_mas' },
			{ col: 'tasa_actividad', labelKey: 'radio.actividad', aggregation: 'mean', unit: '%', baseCol: 'pob_14_mas' },
			{ col: 'pct_universitario', labelKey: 'radio.universitario', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'viirs_mean_radiance', labelKey: 'radio.viirs', aggregation: 'mean', unit: 'nW/cm²/sr' },
			{ col: 'building_density_per_km2', labelKey: 'radio.buildingDensity', aggregation: 'mean', unit: 'edif/km²' },
			// Ranking tabs Ocupados and Desocupación, per cell (pipeline/add_econ_hex_derived.py); not in the tables.
			{ col: 'ocupados_est', labelKey: 'radio.ocupadosEst', aggregation: 'sum', unit: 'personas', baseCol: 'pob_14_mas', hidden: true },
			{ col: 'tasa_desocupacion', labelKey: 'radio.desocupacion', aggregation: 'mean', unit: '%', baseCol: 'pob_14_mas', hidden: true },
			// Year-base values for the transition charts (pipeline/build_econ_trayectorias.py), not shown in tables.
			{ col: 'pct_universitario_2010', labelKey: 'radio.universitario', aggregation: 'mean', unit: '%', hidden: true },
			{ col: 'viirs_2014_neto', labelKey: 'radio.viirs', aggregation: 'mean', unit: 'nW/cm²/sr', hidden: true },
			{ col: 'viirs_2025_neto', labelKey: 'radio.viirs', aggregation: 'mean', unit: 'nW/cm²/sr', hidden: true },
		],
		tabFlows: {
			pct_universitario: {
				periods: { baseline: '2010', current: '2022' },
				bands: {
					col: 'pct_universitario', baselineCol: 'pct_universitario_2010', breaks: [2, 5, 10],
					labels: ['flow.univ.b1', 'flow.univ.b2', 'flow.univ.b3', 'flow.univ.b4'],
					colors: [MAP_SELVA[1], MAP_SELVA[2], MAP_SELVA[4], MAP_SELVA[5]],
					higherIsWorse: false, unit: '%', upLabel: 'flow.univ.up', downLabel: 'flow.univ.down', note: 'flow.univ.note',
				},
			},
			viirs_mean_radiance: {
				periods: { baseline: '2014', current: '2025' },
				bands: {
					col: 'viirs_2025_neto', baselineCol: 'viirs_2014_neto', breaks: [1, 10],
					labels: ['flow.viirs.b1', 'flow.viirs.b2', 'flow.viirs.b3'],
					colors: [...MAP_VIRIDIS_3],
					higherIsWorse: false, unit: 'nW/cm²/sr', upLabel: 'flow.viirs.up', downLabel: 'flow.viirs.down', note: 'flow.viirs.note',
				},
			},
		},
		primaryVariable: 'viirs_mean_radiance',
		colorScale: 'night',
		aggregation: 'mean',
		titleKey: 'analysis.economic.title',
		perDepartment: true,
		legendLowKey: 'legend.economic.low',
		legendHighKey: 'legend.economic.high',
		variableClasses: { ocupados_est: OCUP_HEX_CLASSES, tasa_desocupacion: DESOCUP_CLASSES },
		variableLegend: {
			tasa_empleo: { low: 'legend.empleo.low', high: 'legend.empleo.high' },
			pct_universitario: { low: 'legend.univ.low', high: 'legend.univ.high' },
		},
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	accessibility: {
		id: 'accessibility',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'min_cabecera', unit: 'min', decimals: 0, hexVariable: 'travel_min_cabecera_raw',
			titleKey: 'rank.access.title', noteKey: 'rank.access.note',
			metrics: [
				{ tabKey: 'rank.access.tab.cabecera' },
				{ tabKey: 'rank.access.tab.capital', field: 'min_capital', hexVariable: 'travel_min_capital_raw' },
				{ tabKey: 'rank.access.tab.hospital', field: 'min_hospital', noteKey: 'rank.access.note.hospital', hexVariable: 'travel_min_hospital_raw' },
			],
		},
		parquet: 'sat_accessibility',
		// Every column: minutes by road (Oxford/MAP motorised friction 2019, least cost;
		// cabecera = Oxford accessibility_to_cities 2015). Hospital, salud and secundaria
		// exist only in Argentina (IGN/SISA, Mapa Educativo Nacional): NULL in Paraguay.
		variables: [
			{ col: 'travel_min_capital_raw', unit: 'min', labelKey: 'radio.travelCapital', aggregation: 'mean' },
			{ col: 'travel_min_cabecera_raw', unit: 'min', labelKey: 'radio.travelCabecera', aggregation: 'mean' },
			{ col: 'travel_min_hospital_raw', unit: 'min', labelKey: 'radio.travelHospital', aggregation: 'mean' },
			{ col: 'travel_min_salud_raw', unit: 'min', labelKey: 'radio.travelSalud', aggregation: 'mean' },
			{ col: 'travel_min_secundaria_raw', unit: 'min', labelKey: 'radio.travelSecundaria', aggregation: 'mean' },
			{ col: 'travel_min_ruta_raw', unit: 'min', labelKey: 'radio.travelRuta', aggregation: 'mean' },
		],
		primaryVariable: 'travel_min_capital_raw',
		colorScale: 'flood',
		colorClasses: ACCESS_TIME_CLASSES,
		variableClasses: {
			travel_min_hospital_raw: ACCESS_NEAR_CLASSES,
			travel_min_salud_raw: ACCESS_NEAR_CLASSES,
			travel_min_secundaria_raw: ACCESS_NEAR_CLASSES,
			travel_min_ruta_raw: ACCESS_NEAR_CLASSES,
		},
		aggregation: 'mean',
		titleKey: 'analysis.accessibility.title',
		perDepartment: true,
		legendLowKey: 'legend.accessibility.low',
		legendHighKey: 'legend.accessibility.high',
		// Each ranking tab paints its own column: the capital's labels misread the others.
		variableLegend: {
			travel_min_cabecera_raw: { low: 'legend.accessCity.low', high: 'legend.accessCity.high' },
			travel_min_hospital_raw: { low: 'legend.accessHosp.low', high: 'legend.accessHosp.high' },
		},
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', amambay_py: 'unavailable'},
	},
	// ── PM2.5 (ACAG V6 annual mean, µg/m³; 2001–2010 vs 2013–2022) ──
	pm25_drivers: {
		id: 'pm25_drivers',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'pm25_ug', unit: 'µg/m³', decimals: 1,
			titleKey: 'rank.pm25.title', noteKey: 'rank.pm25.note',
			extraField: 'pm25_delta', extraUnitKey: 'rank.unit.ugDelta', extraDecimals: 1,
		},
		parquet: 'sat_pm25_drivers',
		variables: [
			{ col: 'type', labelKey: 'sat.pm25.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.pm25.typeLabel', aggregation: 'mean' },
			{ col: 'c_pm25_mean', unit: 'µg/m³', labelKey: 'sat.pm25.pm25Mean', aggregation: 'mean' },
		],
		primaryVariable: 'c_pm25_mean',
		legendLowKey: 'legend.pm25.low',
		legendHighKey: 'legend.pm25.high',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.pm25.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2013–2022', baseline: '2001–2010', source: 'ACAG V6' },
		// Bands on RAW µg/m³ anchored to WHO 2021 AQG interim targets (IT-4..IT-1),
		// matching the parquet typology in compute_pm25_drivers.py.
		flowBands: {
			col: 'c_pm25_mean',
			baselineCol: 'c_pm25_mean_baseline',
			breaks: [10, 15, 25, 35],
			labels: ['flow.cls.veryLow', 'flow.cls.low', 'flow.cls.moderate', 'flow.cls.high', 'flow.cls.veryHigh'],
			colors: [...MAP_CLASS_5],
			higherIsWorse: true,
			unit: 'µg/m³',
			note: 'flow.pm25.note',
		},
		// Regional range covers every territory (7.4–19.2 µg/m³ in pm25_scale.json).
		regionalColorDomain: [7, 20],
		scaleStats: PM25_SCALE,
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	// ── Deforestation Dynamics (Hansen 2001-2025, observed) ──
	// No petalVars: only 2 numeric indicators (loss_rate + cumulative) — radar with 2 vars is not meaningful.
	deforestation_dynamics: {
		id: 'deforestation_dynamics',
		parquet: 'sat_deforestation_dynamics',
		variables: [
			{ col: 'c_loss_rate', unit: '%/yr', labelKey: 'sat.deforest.lossRate', aggregation: 'mean' },
			{ col: 'c_cumulative', unit: '%', labelKey: 'sat.deforest.cumulative', aggregation: 'mean' },
			// % of the 2000 tree cover lost per year, per cell (pipeline/add_deforest_hex_derived.py, 5-nov-2026):
			// the «% del bosque» tab's map. Empty where the cell had < 10 % tree cover in 2000.
			{ col: 'c_loss_pct_forest', unit: '%/yr', labelKey: 'sat.deforest.lossPctForest', aggregation: 'mean', hidden: true },
		],
		// No type chip: its old score-cut classes ("Baja presión" up to 1.25 %/yr) contradicted
		// the legend's classes (3-nov-2026 audit).
		primaryVariable: 'c_loss_rate',
		legendLowKey: 'legend.deforestLoss.low',
		legendHighKey: 'legend.deforestLoss.high',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.deforest.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2016–2025', baseline: '2001–2010', source: 'Hansen GFC v1.13' },
		// Three readings of the same Hansen loss (pipeline/build_deforest_rank.py):
		// where the most is lost, where it is most intense, where it sped up or slowed.
		// Open department: Hectáreas paints c_loss_rate (equal-area cells: the same reading as ha/yr),
		// % del bosque its own column, Aceleración the temporal change of c_loss_rate (temporalMode).
		deptRank: {
			field: 'loss_ha_yr', unit: 'ha/año', unitKey: 'rank.unit.haYr', decimals: 0,
			titleKey: 'rank.deforest.title.ha', noteKey: 'rank.deforest.note.ha', hexVariable: 'c_loss_rate',
			metrics: [
				{ tabKey: 'rank.deforest.tab.ha' },
				{ tabKey: 'rank.deforest.tab.pct', field: 'loss_pct_forest', unit: '%/año', unitKey: 'rank.unit.pctYr', decimals: 2, hexVariable: 'c_loss_pct_forest',
				  titleKey: 'rank.deforest.title.pct', noteKey: 'rank.deforest.note.pct',
				  extraField: 'loss_ha_yr', extraUnitKey: 'rank.unit.haYr', extraDecimals: 0 },
				{ tabKey: 'rank.deforest.tab.change', field: 'loss_change_pct', unit: '%', unitKey: 'rank.unit.pct', decimals: 0, signed: true, temporalMode: 'delta',
				  titleKey: 'rank.deforest.title.change', noteKey: 'rank.deforest.note.change',
				  extraField: 'loss_ha_yr', extraUnitKey: 'rank.unit.haYr', extraDecimals: 0 },
			],
		},
		// Bands on RAW loss rate (%/yr): higher = worse,
		// so band colors and improved/worsened flow semantics are inverted.
		flowBands: {
			col: 'c_loss_rate',
			baselineCol: 'c_loss_rate_baseline',
			breaks: [0.5, 2],
			labels: ['flow.cls.low', 'flow.cls.medium', 'flow.cls.high'],
			colors: [...MAP_CLASS_3],   // = DEFOREST_LOSS_CLASSES
			higherIsWorse: true,
			unit: '%/año',
			note: 'flow.deforest.note',
		},
		colorClasses: DEFOREST_LOSS_CLASSES,
		variableClasses: { c_loss_pct_forest: DEFOREST_LOSS_CLASSES },
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	// ── EUDR: pérdida arbórea posterior a 2020 (H3 res-9, por departamento) ──
	// pipeline/build_eudr_dpto.py parte el grid res-9 de /eudr/check con los departamentos
	// de la capa de deforestación: {prefijo}sat_dpto/sat_eudr_{key}.parquet + resumen con
	// ranking. El grid no llega al norte del Chaco paraguayo: Alto Paraguay sin capa, y
	// en Boquerón Gral. Eugenio A. Garay (5 % de sus hexágonos) queda sin barra.
	// Variable principal = la fracción física (tier 1), no risk_score, que es un
	// promedio ponderado 70/20/10 (lo sigue usando sólo /eudr/check).
	// Dominio de color fijo 0-40 %: P98 en res-9 = 40 % (mediana y P90 = 0); con
	// MIN/MAX (0-100) casi todo el mapa quedaría en el primer color.
	eudr: {
		id: 'eudr',
		parquet: 'sat_eudr',
		variables: [
			{ col: 'loss_post_2020_pct', labelKey: 'eudr.lossPost2020', aggregation: 'mean', unit: '%' },
			{ col: 'fire_native_post_2020_pct', labelKey: 'eudr.fireNativePost2020', aggregation: 'mean', unit: '%' },
			{ col: 'fire_post_2020_pct', labelKey: 'eudr.firePost2020', aggregation: 'mean', unit: '%' },
			{ col: 'forest_cover_2020', labelKey: 'eudr.forest2020', aggregation: 'mean', unit: '%' },
			{ col: 'forest_cover_current', labelKey: 'eudr.forestCurrent', aggregation: 'mean', unit: '%' },
		],
		primaryVariable: 'loss_post_2020_pct',
		colorScale: 'flood',
		fixedColorDomain: [0, 40],
		aggregation: 'mean',
		petalVars: [
			{ col: 'loss_post_2020_pct', labelKey: 'eudr.lossPost2020', aggregation: 'mean' },
			{ col: 'fire_native_post_2020_pct', labelKey: 'eudr.fireNativePost2020', aggregation: 'mean' },
			{ col: 'forest_cover_2020', labelKey: 'eudr.forest2020', aggregation: 'mean' },
			{ col: 'forest_cover_current', labelKey: 'eudr.forestCurrent', aggregation: 'mean' },
		],
		titleKey: 'trade.eudr.analysis_title',
		perDepartment: true,
		// Three readings of the post-2020 loss (pipeline/build_eudr_dpto.py).
		deptRank: {
			field: 'loss_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
			titleKey: 'rank.eudr.title.ha', noteKey: 'rank.eudr.note.ha',
			metrics: [
				{ tabKey: 'rank.deforest.tab.ha' },
				{ tabKey: 'rank.deforest.tab.pct', field: 'loss_pct_forest', unit: '%', unitKey: 'rank.unit.pct', decimals: 1, hexNone: true,
				  titleKey: 'rank.eudr.title.pct', noteKey: 'rank.eudr.note.pct',
				  extraField: 'loss_ha', extraUnitKey: 'rank.unit.ha', extraDecimals: 0 },
				// Per hex: % of the cell burned on native woody vegetation (the hectares of the bar, cell by cell).
				{ tabKey: 'rank.eudr.tab.fire', field: 'fire_native_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0, hexVariable: 'fire_native_post_2020_pct',
				  titleKey: 'rank.eudr.title.fire', noteKey: 'rank.eudr.note.fire' },
			],
		},
		legendLowKey: 'legend.eudr.low',
		legendHighKey: 'legend.eudr.high',
		coverage: { corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'available', itapua_py: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
};

// ── Analysis system ─────────────────────────────────────────────────────────

export interface AnalysisConfig {
	id: string;
	lensId: LensId;
	titleKey: string;
	descKey: string;
	icon: string;
	status: 'available' | 'coming_soon';
	spatialUnit?: 'radio' | 'hexagon' | 'macizo';
	dashboard?: boolean;
	coverage?: Record<string, 'available' | 'pending' | 'unavailable'>;
	comparable?: true;
	rigorBadge?: 'physical' | 'modeled' | 'census';
	choropleth?: {
		parquet: string;
		column: string;
		colorScale: 'price' | 'score' | 'diverging' | 'sequential' | 'flood';
		legendKey: string;
	};
}

// AUTHORITATIVE source for cross-territory comparability (`comparable: true`) and
// menu coverage. The UI reads only this registry (AnalysisMenu, BivariatePlot,
// Sidebar, OvertureAnalysis). Each entry's `coverage` map MUST stay identical to the
// same-id entry in HEX_LAYER_REGISTRY — a dev-only guard below warns on drift.
export const ANALYSIS_REGISTRY: AnalysisConfig[] = [
	// ── Ambiente y riesgo ──
	{
		id: 'flood_risk',
		lensId: 'ambiente',
		titleKey: 'analysis.floodRisk.title',
		descKey: 'analysis.floodRisk.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'deforestation_dynamics',
		lensId: 'ambiente',
		titleKey: 'sat.deforest.title',
		descKey: 'sat.deforest.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'pm25_drivers',
		lensId: 'ambiente',
		titleKey: 'sat.pm25.title',
		descKey: 'sat.pm25.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'modeled',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	// ── Producción y suelo ──
	{
		id: 'agri_potential',
		lensId: 'produccion',
		titleKey: 'sat.agri.title',
		descKey: 'sat.agri.desc',
		coverage: { corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'unavailable', itapua_py: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable' },
		comparable: true,
		rigorBadge: 'modeled',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'forestry_aptitude',
		lensId: 'produccion',
		titleKey: 'sat.forestry.title',
		descKey: 'sat.forestry.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'modeled',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'land_use',
		lensId: 'produccion',
		titleKey: 'sat.landUse.title',
		descKey: 'sat.landUse.desc',
		coverage: { misiones: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'unavailable', itapua_py: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', cordillera_py: 'unavailable', san_pedro_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable' },
		comparable: true,
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	// ── Población y servicios ──
	{
		id: 'accessibility',
		lensId: 'poblacion',
		titleKey: 'analysis.accessibility.title',
		descKey: 'analysis.accessibility.desc',
		status: 'available',
		spatialUnit: 'hexagon',
		// Not comparable: each hexagon is timed to its own capital (province, PY department
		// or BR state), so minutes do not compare across countries (methodology). No
		// `comparable` flag on purpose.
		rigorBadge: 'modeled',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', amambay_py: 'unavailable'},
	},
	{
		id: 'censo_temporal',
		lensId: 'poblacion',
		titleKey: 'analysis.censo.title',
		descKey: 'analysis.censo.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'sociodemographic',
		lensId: 'poblacion',
		titleKey: 'analysis.socio.title',
		descKey: 'analysis.socio.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'service_deprivation',
		lensId: 'poblacion',
		titleKey: 'sat.deprivation.title',
		descKey: 'sat.deprivation.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'health_access',
		lensId: 'poblacion',
		titleKey: 'sat.health.title',
		descKey: 'sat.health.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'education_capital',
		lensId: 'poblacion',
		titleKey: 'sat.eduCap.title',
		descKey: 'sat.eduCap.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'education_flow',
		lensId: 'poblacion',
		titleKey: 'sat.eduFlow.title',
		descKey: 'sat.eduFlow.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		comparable: true,
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	// ── Economía y comercio ──
	{
		id: 'economic_activity',
		lensId: 'economia',
		titleKey: 'analysis.economic.title',
		descKey: 'analysis.economic.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'eudr',
		lensId: 'economia',
		titleKey: 'trade.eudr.analysis_title',
		descKey: 'trade.eudr.analysis_desc',
		// Per-department res-9 split of the cross-border EUDR grid (build_eudr_dpto.py),
		// NOT a comparable analysis. The grid misses the northern Paraguayan Chaco.
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
];

// ── Dev-only invariant: HEX_LAYER_REGISTRY and ANALYSIS_REGISTRY each carry a
// `coverage` map per layer id; they must stay identical. This warns on drift during
// `npm run dev` and is tree-shaken out of the production build (import.meta.env.DEV).
if (import.meta.env.DEV) {
	for (const a of ANALYSIS_REGISTRY) {
		const hex = HEX_LAYER_REGISTRY[a.id];
		if (!hex?.coverage || !a.coverage) continue;
		const keys = new Set([...Object.keys(hex.coverage), ...Object.keys(a.coverage)]);
		for (const k of keys) {
			if (hex.coverage[k] !== a.coverage[k]) {
				console.warn(
					`[config] coverage drift "${a.id}" @ "${k}": HEX=${hex.coverage[k]} ANALYSIS=${a.coverage[k]}`
				);
			}
		}
	}
}

// ── Territorial Scores definitions ──────────────────────────────────────────
export const TERRITORIAL_SCORE_COLS = [
	'paving_index', 'urban_consolidation', 'service_access',
	'commercial_vitality', 'road_connectivity', 'building_mix',
	'urbanization', 'water_exposure',
] as const;

export const TERRITORIAL_COMPONENT_COLS = [
	'building_count', 'n_paved', 'n_unpaved', 'place_count',
	'segment_count', 'water_kring_total',
] as const;




export function getAnalysesForLens(lensId: LensId): AnalysisConfig[] {
	return ANALYSIS_REGISTRY.filter(a => a.lensId === lensId);
}


export function getAnalysisById(id: string): AnalysisConfig | undefined {
	return ANALYSIS_REGISTRY.find(a => a.id === id);
}

// ── Data freshness metadata ─────────────────────────────────────────────────

export const DATA_FRESHNESS: Record<string, { dataDate: string; processedDate: string; sourceKey: string }> = {
	hex_flood_risk: {
		dataDate: '21/03/2026', // fecha real de la imagen Sentinel-1 SAR horneada (no el mes de procesamiento)
		processedDate: '21/04/2026',
		sourceKey: 'analysis.flood.source',
	},
	censo_radios: {
		dataDate: 'Censo 2022',
		processedDate: '21/03/2026',
		sourceKey: 'data.source.censo',
	},
	buildings_stats: {
		dataDate: '02/2025',
		processedDate: '21/03/2026',
		sourceKey: 'data.source.buildings',
	},
	// sourceKey = the layer's own datasets, from its published methodology (no
	// generic «satellite» list). processedDate = last reprocess (git) or, where
	// none is recorded, the publication date of the file in R2.
	sat_agri_apt: { dataDate: 'CHIRPS v3 1981–2026 · ERA5-Land corregido con 48 estaciones 1950–2026 · SoilGrids 2.0 · FABDEM · normal 1991–2020', processedDate: '01/11/2026', sourceKey: 'layer.source.agri' },
	sat_forestry_aptitude: { dataDate: 'ERA5/CHIRPS/SoilGrids/SRTM 2019-2023', processedDate: '20/06/2026', sourceKey: 'layer.source.forestry' },
	sat_service_deprivation: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '30/10/2026', sourceKey: 'layer.source.census2022' },
	sat_health_access: { dataDate: 'Oxford MAP 2019 + OSM + Censo 2022', processedDate: '30/10/2026', sourceKey: 'layer.source.health' },
	sat_education_capital: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '30/10/2026', sourceKey: 'layer.source.census2022' },
	sat_education_flow: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '30/10/2026', sourceKey: 'layer.source.census2022' },
	sat_sociodemographic: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '30/10/2026', sourceKey: 'layer.source.census2022' },
	sat_censo_temporal: { dataDate: 'Censos INDEC 1991 · 2001 · 2010 · 2022', processedDate: '10/06/2026', sourceKey: 'layer.source.censoTemporal' },
	sat_economic_activity: { dataDate: 'Censo 2022 + VIIRS 2025 + GBA 2025', processedDate: '02/11/2026', sourceKey: 'layer.source.economic' },
	sat_accessibility: { dataDate: 'Oxford MAP 2019 / IGN-SISA / Mapa Educativo / OSM', processedDate: '05/11/2026', sourceKey: 'layer.source.accessibility' },
	sat_uso_suelo: { dataDate: 'MapBiomas Argentina Colección 3, 1985–2025', processedDate: '03/11/2026', sourceKey: 'layer.source.landUse' },
	sat_pm25_drivers: { dataDate: 'ACAG V6.GL.02 1998-2022', processedDate: '01/07/2026', sourceKey: 'layer.source.pm25' },
	sat_deforestation_dynamics: { dataDate: 'Hansen GFC v1.13 2001-2025', processedDate: '02/11/2026', sourceKey: 'layer.source.deforestation' },
	// EUDR: los parquets por depto salen del grid res-9 (build_eudr_dpto.py, 30/10/2026); el refresh mensual del grid está en data/eudr/meta.json.
	sat_eudr: { dataDate: 'Hansen GFC v1.13 (2021-2025) + MODIS MCD64A1 x MapBiomas Col.2 (cutoff 31/12/2020)', processedDate: '02/11/2026', sourceKey: 'layer.source.eudr' },
};

// ── EUDR Configuration ──────────────────────────────────────────────────
export const MAP_EUDR = {
	center: [-62.5, -26.5] as [number, number],
	zoom: 5.5,
	minZoom: 4,
	maxZoom: 14,
};

const R2_EUDR_BASE = `${R2_PROD}/data/eudr`;

export function getEudrParquetUrl(name: string): string {
	// v=28: el termino de fuego del score pasa a contar solo el area quemada sobre
	// vegetacion nativa lenosa (MapBiomas clases 3/4/5/6). El fuego crudo se conserva
	// como columna para poder contrastarlos.
	return `${R2_EUDR_BASE}/${name}.parquet?v=28`;
}

export const EUDR_RISK_COLORS = {
	low: '#22c55e',
	medium: '#f59e0b',
	high: '#ef4444',
	critical: '#991b1b',
	unknown: '#6b7280',
};
