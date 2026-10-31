import { unitLabel } from '$lib/utils/units';
import { i18n } from '$lib/stores/i18n.svelte';
import pm25Scale from '$lib/content/pm25_scale.json';
const R2_PROD = 'https://cdn.spatia.ar';

function getBase(): string {
	return R2_PROD;
}

export function getTilesUrl(name: 'buildings' | 'misiones_plantations' | 'corrientes_plantations' | 'chaco_plantations' | 'formosa_plantations' | 'itapua_buildings' | 'itapua_districts' | 'corrientes_buildings' | 'alto_parana_buildings' | 'alto_parana_districts' | 'chaco_buildings' | 'formosa_buildings' | 'parana_br_buildings' | 'santa_catarina_br_buildings' | 'rio_grande_sul_br_buildings' | 'radios' | 'terrain' | 'hexagons' | 'catastro' | 'concepcion_py_buildings' | 'concepcion_py_districts' | 'san_pedro_py_buildings' | 'san_pedro_py_districts' | 'cordillera_py_buildings' | 'cordillera_py_districts' | 'guaira_py_buildings' | 'guaira_py_districts' | 'caaguazu_py_buildings' | 'caaguazu_py_districts' | 'caazapa_py_buildings' | 'caazapa_py_districts' | 'misiones_py_buildings' | 'misiones_py_districts' | 'paraguari_py_buildings' | 'paraguari_py_districts' | 'central_py_buildings' | 'central_py_districts' | 'neembucu_py_buildings' | 'neembucu_py_districts' | 'amambay_py_buildings' | 'amambay_py_districts' | 'canindeyu_py_buildings' | 'canindeyu_py_districts' | 'presidente_hayes_py_buildings' | 'presidente_hayes_py_districts' | 'boqueron_py_buildings' | 'boqueron_py_districts' | 'alto_paraguay_py_buildings' | 'alto_paraguay_py_districts'): string {
	if (name === 'terrain') {
		// Mapterhorn (Copernicus GLO-30 in the region) cut to TERRAIN_DEM.bounds, z0-11,
		// one object per tile so each is edge-cached (a single .pmtiles exceeds the 512 MB cache cap).
		return `${getBase()}/data/terrain/mapterhorn-v1/{z}/{x}/{y}.webp`;
	}
	const files: Record<string, string> = {
		buildings: 'tiles/buildings-v5.pmtiles',
		misiones_plantations: 'data/tiles/misiones_plantations.pmtiles',
		corrientes_plantations: 'data/tiles/corrientes_plantations.pmtiles',
		chaco_plantations: 'data/tiles/chaco_plantations.pmtiles',
		formosa_plantations: 'data/tiles/formosa_plantations.pmtiles',
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
		radios: 'data/tiles/radios-v4.pmtiles',
		hexagons: 'tiles/hexagons-v2.pmtiles',
		catastro: 'tiles/catastro.pmtiles?v=3',
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
		hex_flood_risk: '?v=39',
		sat_agri_potential: '?v=36',
		sat_forestry_aptitude: '?v=40',
		sat_service_deprivation: '?v=32',
		sat_health_access: '?v=30',
		sat_education_capital: '?v=29',
		sat_education_flow: '?v=29',
		sat_sociodemographic: '?v=25',
		sat_economic_activity: '?v=31',
		sat_accessibility: '?v=27',
		sat_pm25_drivers: '?v=19',
		sat_deforestation_dynamics: '?v=16',
		sat_land_use: '?v=5',
		sat_censo_temporal: '?v=1',
		sat_eudr: '?v=1',
	};
	const bust = busts[name] || '';
	return `${getBase()}/data/${name}.parquet${bust}`;
}

// Bump DEPT_V after any pipeline run that regenerates dept-split parquets.
// Cloudflare CDN caches stable versioned URLs; a new version invalidates naturally.
const DEPT_V = 26;

export function getFloodDptoUrl(parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}flood_dpto/hex_flood_${parquetKey}.parquet?v=${DEPT_V}`;
}

export function getScoresDptoUrl(parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}scores_dpto/overture_scores_${parquetKey}.parquet?v=${DEPT_V}`;
}

export function getSatDptoUrl(analysisId: string, parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}sat_dpto/sat_${analysisId}_${parquetKey}.parquet?v=${DEPT_V}`;
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
	get catastro_by_radio() { return getParquetUrl('catastro_by_radio'); },
	get catastro_changes() { return getParquetUrl('catastro_changes_summary'); },
	get overture_buildings() { return getParquetUrl('overture_buildings'); },
	get overture_transportation() { return getParquetUrl('overture_transportation'); },
	get overture_places() { return getParquetUrl('overture_places'); },
	get overture_base() { return getParquetUrl('overture_base'); },
	// Satellite composite scores
	get sat_agri_potential() { return getParquetUrl('sat_agri_potential'); },
	get sat_forestry_aptitude() { return getParquetUrl('sat_forestry_aptitude'); },
	get sat_service_deprivation() { return getParquetUrl('sat_service_deprivation'); },
	get sat_land_use() { return getParquetUrl('sat_land_use'); },
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
	ambiente:   { label: { es: 'Ambiente y riesgo',          en: 'Environment & risk',        gn: 'Yvypóra',    pt: 'Ambiente e risco'          }, color: '#ef4444' },
	produccion: { label: { es: 'Producción y suelo',         en: 'Production & land',         gn: "Mba'apo",    pt: 'Produção e solo'            }, color: '#22c55e' },
	poblacion:  { label: { es: 'Población y servicios',      en: 'Population & services',     gn: 'Tetã kuéra', pt: 'População e serviços'       }, color: '#3b82f6' },
	economia:   { label: { es: 'Economía e infraestructura', en: 'Economy & infrastructure',  gn: 'Moĩ viru',   pt: 'Economia e infraestrutura'  }, color: '#f59e0b' },
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
	// (map, legend and catastro parcels). Ignored in temporal delta mode.
	colorClasses?: ColorClasses;
	// Classes for other variables the map can paint instead of the primary
	// (hexStore.colorVariable, set from the active ranking tab).
	variableClasses?: Record<string, ColorClasses>;
	// Two color scales, switched from the legend (URL ?escala=regional):
	// 'provincial' (default) = P2–P98 of the active territory from scaleStats —
	// the province/department/state is the governing unit; 'regional' = this
	// fixed range, so one color means one value in every territory.
	regionalColorDomain?: [number, number];
	scaleStats?: ScaleStats;
	// FlowChart banding over a RAW variable (raw-data-first convention). When
	// absent, FlowChart falls back to score/score_baseline with 33/67 cuts.
	flowBands?: FlowBands;
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
	// Metric used instead where the summary flags saturation and metadata.rank.plain.shown.
	plain?: { field: string; unit: string; decimals: number; titleKey: string; noteKey: string };
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
	// true → a cut belongs to the class below (value > from), matching FlowChart
	// bands; default → to the class above (value >= from).
	upperInclusive?: boolean;
}

export function colorForClass(value: number, classes: ColorClasses): string {
	if (classes.zero && (value === 0 || (classes.zeroBelow !== undefined && Math.abs(value) < classes.zeroBelow))) return classes.zero.color;
	let color = classes.steps[0].color;
	for (const s of classes.steps) if (classes.upperInclusive ? value > s.from : value >= s.from) color = s.color;
	return color;
}

// JRC occurrence (% of 1984–2021 with water). Cuts match the methodology text
// (<5 / 5–20 / >20). 0 % = Landsat never saw water there — a real reading, not a
// coverage gap: it was painted "Sin cobertura" (97 % of Eldorado) by the legacy
// value-0-as-nodata rule in Map.computeHexColor.
export const FLOOD_OCCURRENCE_CLASSES: ColorClasses = {
	zero: { color: '#1e293b', labelKey: 'legend.flood.none' },
	steps: [
		{ from: 0, color: '#3b82f6', labelKey: 'legend.flood.low' },
		{ from: 5, color: '#eab308', labelKey: 'legend.flood.mid' },
		{ from: 20, color: '#dc2626', labelKey: 'legend.flood.high' },
	],
};

// Per-hex relief variables, painted when a flood ranking tab is active
// (deptRank.hexVariable). Same cuts in every territory. Cuts from the Misiones
// distribution of cells with any building on low ground (p50 2, p75 7, p90 16,
// p99 139) and of added buildings there 2016–2023 (p50 1, p75 3, p90 7, p99 41).
export const FLOOD_LOW_BLD_CLASSES: ColorClasses = {
	zero: { color: '#1e293b', labelKey: 'legend.lowBld.none' },
	zeroBelow: 0.5,
	steps: [
		{ from: 0.5, color: '#60a5fa', labelKey: 'legend.lowBld.c1' },
		{ from: 2.5, color: '#eab308', labelKey: 'legend.lowBld.c2' },
		{ from: 9.5, color: '#f97316', labelKey: 'legend.lowBld.c3' },
		{ from: 49.5, color: '#dc2626', labelKey: 'legend.lowBld.c4' },
	],
};
export const FLOOD_LOW_LAND_CLASSES: ColorClasses = {
	zero: { color: '#1e293b', labelKey: 'legend.lowLand.none' },
	steps: [
		{ from: 0, color: '#3b82f6', labelKey: 'legend.lowLand.c1' },
		{ from: 25, color: '#eab308', labelKey: 'legend.lowLand.c2' },
		{ from: 75, color: '#dc2626', labelKey: 'legend.lowLand.c3' },
	],
};
export const FLOOD_LOW_NEW_CLASSES: ColorClasses = {
	zero: { color: '#1e293b', labelKey: 'legend.lowNew.none' },
	zeroBelow: 0.5,
	steps: [
		{ from: -1e9, color: '#64748b', labelKey: 'legend.lowNew.down' },
		{ from: 0.5, color: '#60a5fa', labelKey: 'legend.lowNew.c1' },
		{ from: 4.5, color: '#eab308', labelKey: 'legend.lowNew.c2' },
		{ from: 19.5, color: '#dc2626', labelKey: 'legend.lowNew.c3' },
	],
};

// Hansen loss rate (%/yr, 2015–2024). Same cuts and colors as the layer's
// flowBands so map and flow chart classify a hexagon alike. 0 = no loss
// detected — in Eldorado half the hexagons, median tree cover 98 %: standing
// forest, previously painted "Sin cobertura".
export const DEFOREST_LOSS_CLASSES: ColorClasses = {
	zero: { color: '#1e293b', labelKey: 'legend.deforest.none' },
	steps: [
		{ from: 0, color: '#22d3ee', labelKey: 'legend.deforest.low' },
		{ from: 0.5, color: '#f59e0b', labelKey: 'legend.deforest.mid' },
		{ from: 2, color: '#f87171', labelKey: 'legend.deforest.high' },
	],
	upperInclusive: true,
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
	// Genuine 0-100 composite score (eudr risk_score, agri c_ph_optimal) → '/100'.
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
			{ col: 'low_bld_n', labelKey: 'analysis.flood.lowBld', aggregation: 'sum', unit: 'edificios' },
			{ col: 'low_land_pct', labelKey: 'analysis.flood.lowLand', aggregation: 'mean', unit: '%' },
			{ col: 'low_bld_new_n', labelKey: 'analysis.flood.lowBldNew', aggregation: 'sum', unit: 'edificios' },
		],
		primaryVariable: 'jrc_occurrence',
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
		variableClasses: { low_bld_n: FLOOD_LOW_BLD_CLASSES, low_land_pct: FLOOD_LOW_LAND_CLASSES, low_bld_new_n: FLOOD_LOW_NEW_CLASSES },
		// Exposure by terrain (HAND < 5 m), not JRC: JRC occurrence ranks river and
		// reservoir shores (Capital on Yacyretá, Iguazú on Urugua-í) and misses the
		// short floods of the plateau. pipeline/build_flood_rank.py.
		deptRank: {
			field: 'bld_hand5_pct', unit: '%', decimals: 1,
			titleKey: 'rank.flood.title', noteKey: 'rank.flood.note',
			extraField: 'land_hand5_pct', extraUnitKey: 'rank.unit.land', extraDecimals: 1,
			saturatedKey: 'rank.flood.saturated',
			hexVariable: 'low_bld_n',
			// Chaco plain: recurrent waterlogging by radar (HAND < 1 m ranked it backwards).
			plain: { field: 's1_land_pct', unit: '%', decimals: 1, titleKey: 'rank.flood.plain.title', noteKey: 'rank.flood.plain.note' },
			// Three readings of the same HAND export: share of buildings (default),
			// number of buildings (magnitude, follows population), share of land (rural).
			metrics: [
				{ tabKey: 'rank.flood.tab.pct' },
				{ tabKey: 'rank.flood.tab.n', field: 'bld_hand5_n', unit: '', unitKey: 'rank.unit.buildings', decimals: 0,
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
	},
	// ── Satellite composite scores ──
	agri_potential: {
		id: 'agri_potential',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'high_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
			titleKey: 'rank.agri.title', noteKey: 'rank.agri.note',
			extraField: 'high_pct', extraUnitKey: 'rank.unit.pctArea', extraDecimals: 1,
		},
		parquet: 'sat_agri_potential',
		variables: [
			{ col: 'c_soc', labelKey: 'sat.agri.soc', aggregation: 'mean', unit: '/100' },
			{ col: 'c_ph_optimal', labelKey: 'sat.agri.phOptimal', aggregation: 'mean', unit: '/100' },
			{ col: 'c_clay', labelKey: 'sat.agri.clay', aggregation: 'mean', unit: '/100' },
			{ col: 'c_precipitation', labelKey: 'sat.agri.precip', aggregation: 'mean', unit: '/100' },
			{ col: 'c_gdd', labelKey: 'sat.agri.gdd', aggregation: 'mean', unit: '/100' },
			{ col: 'c_slope', labelKey: 'sat.agri.slope', aggregation: 'mean', unit: '/100' },
			{ col: 'score', labelKey: 'sat.agri.score', aggregation: 'mean', unit: '/100' },
		],
		primaryVariable: 'c_ph_optimal',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.agri.title',
		perDepartment: true,
		legendLowKey: 'legend.agri.low',
		legendHighKey: 'legend.agri.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
	},
	service_deprivation: {
		id: 'service_deprivation',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'nbi_pct', unit: '%', decimals: 1,
			titleKey: 'rank.service.title', noteKey: 'rank.service.note',
			extraField: 'nbi_n', extraUnitKey: 'rank.unit.hogares', extraDecimals: 0,
			metrics: [
				{ tabKey: 'rank.service.tab.nbi' },
				{ tabKey: 'rank.service.tab.cloacas', field: 'cloacas_pct', extraField: 'cloacas_n' },
				{ tabKey: 'rank.service.tab.hacin', field: 'hacin_pct', extraField: 'hacin_n' },
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
			field: 'noschool_pct', unit: '%', decimals: 1,
			titleKey: 'rank.educap.title', noteKey: 'rank.educap.note',
			metrics: [
				{ tabKey: 'rank.educap.tab.noschool' },
				{ tabKey: 'rank.educap.tab.univ', field: 'univ_pct' },
			],
		},
		parquet: 'sat_education_capital',
		variables: [
			// Real Census 2022 rates, % of people aged 18+ (pipeline/build_census_rates.py).
			{ col: 'c_no_schooling', labelKey: 'sat.eduCap.noSchooling', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_secondary_plus', labelKey: 'sat.eduCap.secondaryPlus', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_higher_edu', labelKey: 'sat.eduCap.higherEdu', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
			{ col: 'c_university', labelKey: 'sat.eduCap.university', aggregation: 'mean', unit: '%', baseCol: 'pob_18_mas' },
		],
		quantileDomain: true,
		primaryVariable: 'c_no_schooling',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.eduCap.title',
		perDepartment: true,
		legendLowKey: 'legend.eduCap.low',
		legendHighKey: 'legend.eduCap.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	education_flow: {
		id: 'education_flow',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'dropout_sec_pct', unit: '%', decimals: 1,
			titleKey: 'rank.eduflow.title', noteKey: 'rank.eduflow.note',
			metrics: [
				{ tabKey: 'rank.eduflow.tab.dropout' },
				{ tabKey: 'rank.eduflow.tab.teen', field: 'teen_preg_pct' },
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
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'trees_pct', unit: '%', decimals: 1,
			titleKey: 'rank.landuse.title', noteKey: 'rank.landuse.note.trees',
			extraField: 'trees_ha', extraUnitKey: 'rank.unit.ha', extraDecimals: 0,
			metrics: [
				{ tabKey: 'rank.landuse.tab.trees' },
				{ tabKey: 'rank.landuse.tab.crops', field: 'crops_pct', extraField: 'crops_ha', noteKey: 'rank.landuse.note.crops' },
				{ tabKey: 'rank.landuse.tab.built', field: 'built_pct', extraField: 'built_ha', noteKey: 'rank.landuse.note.built' },
			],
		},
		parquet: 'sat_land_use',
		// Dynamic World v1 (Google/WRI, 10m): 9 land-cover probability fractions [0-1].
		// Global product, identical method across AR/PY/BR → cross-territory comparable.
		// (Earlier config declared MapBiomas cols that do not exist in the parquet, so
		// every hex rendered as "Sin cobertura"; realigned to the real Dynamic World schema.)
		variables: [
			{ col: 'frac_trees',   labelKey: 'sat.landUse.trees',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_crops',   labelKey: 'sat.landUse.crops',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_grass',   labelKey: 'sat.landUse.grass',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_shrub',   labelKey: 'sat.landUse.shrub',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_built',   labelKey: 'sat.landUse.built',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_water',   labelKey: 'sat.landUse.water',   aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_flooded', labelKey: 'sat.landUse.flooded', aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_bare',    labelKey: 'sat.landUse.bare',    aggregation: 'mean', unit: '%', scale: 100 },
			{ col: 'frac_snow',    labelKey: 'sat.landUse.snow',    aggregation: 'mean', unit: '%', scale: 100 },
		],
		primaryVariable: 'frac_trees',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.landUse.title',
		perDepartment: true,
		legendLowKey: 'legend.landUse.low',
		legendHighKey: 'legend.landUse.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', cordillera_py: 'available', san_pedro_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
	},
	// ── Migrated from radio/catastro to H3 ──
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
			labels: ['Rural', 'Cluster urbano', 'Centro urbano'],
			colors: ['#a3e635', '#f59e0b', '#22d3ee'],
			higherIsWorse: false,
			upLabel: 'densificaron',
			downLabel: 'perdieron densidad',
			note: 'Bandas de densidad (hab/km²) según Grado de Urbanización (DEGURBA, ONU 2020): Rural (<300) · Cluster urbano (300–1.500) · Centro urbano (≥1.500) · Clic en un flujo o banda para ver esos hexágonos en el mapa',
		},
		legendLowKey: 'legend.censo.low',
		legendHighKey: 'legend.censo.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	economic_activity: {
		id: 'economic_activity',
		// Department ranking — pipeline/build_layer_ranks.py: Σ rate·base / Σ base over
		// the department's hexagons (same INDEC definition in the 4 provinces).
		deptRank: {
			field: 'empleo_pct', unit: '%', decimals: 1,
			titleKey: 'rank.econ.title', noteKey: 'rank.econ.note',
			metrics: [
				{ tabKey: 'rank.econ.tab.empleo' },
				{ tabKey: 'rank.econ.tab.univ', field: 'univ_pct', noteKey: 'rank.econ.note.univ' },
				{ tabKey: 'rank.econ.tab.viirs', field: 'viirs', unit: 'nW/cm²/sr', decimals: 2, noteKey: 'rank.econ.note.viirs' },
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
		],
		primaryVariable: 'viirs_mean_radiance',
		colorScale: 'night',
		aggregation: 'mean',
		titleKey: 'analysis.economic.title',
		perDepartment: true,
		legendLowKey: 'legend.economic.low',
		legendHighKey: 'legend.economic.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable', concepcion_py: 'unavailable', san_pedro_py: 'unavailable', cordillera_py: 'unavailable', guaira_py: 'unavailable', caaguazu_py: 'unavailable', caazapa_py: 'unavailable', misiones_py: 'unavailable', paraguari_py: 'unavailable', central_py: 'unavailable', neembucu_py: 'unavailable', amambay_py: 'unavailable', canindeyu_py: 'unavailable', presidente_hayes_py: 'unavailable', boqueron_py: 'unavailable', alto_paraguay_py: 'unavailable'},
	},
	accessibility: {
		id: 'accessibility',
		// Department ranking — pipeline/build_layer_ranks.py.
		deptRank: {
			field: 'min_cabecera', unit: 'min', decimals: 0,
			titleKey: 'rank.access.title', noteKey: 'rank.access.note',
			metrics: [
				{ tabKey: 'rank.access.tab.cabecera' },
				{ tabKey: 'rank.access.tab.capital', field: 'min_capital' },
				{ tabKey: 'rank.access.tab.hospital', field: 'km_hospital', unit: 'km', decimals: 1, noteKey: 'rank.access.note.hospital' },
			],
		},
		parquet: 'sat_accessibility',
		variables: [
			{ col: 'travel_min_capital', rawCol: 'travel_min_capital_raw', unit: 'min', labelKey: 'radio.travelCapital', aggregation: 'mean' },
			{ col: 'travel_min_cabecera', rawCol: 'travel_min_cabecera_raw', unit: 'min', labelKey: 'radio.travelCabecera', aggregation: 'mean' },
			{ col: 'dist_nearest_hospital_km', rawCol: 'dist_nearest_hospital_km_raw', unit: 'km', labelKey: 'radio.distHospital', aggregation: 'mean' },
			{ col: 'dist_nearest_secundaria_km', rawCol: 'dist_nearest_secundaria_km_raw', unit: 'km', labelKey: 'radio.distSecundaria', aggregation: 'mean' },
			{ col: 'dist_primary_m', rawCol: 'dist_primary_m_raw', unit: 'km', labelKey: 'radio.distPrimaria', aggregation: 'mean' },
		],
		primaryVariable: 'travel_min_capital_raw',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'analysis.accessibility.title',
		perDepartment: true,
		legendLowKey: 'legend.accessibility.low',
		legendHighKey: 'legend.accessibility.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', central_py: 'available', neembucu_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available', misiones_py: 'available', paraguari_py: 'available', amambay_py: 'available'},
	},
	// ── PM2.5 Drivers (25-year ML decomposition) ──
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
			labels: ['Muy baja', 'Baja', 'Moderada', 'Alta', 'Muy alta'],
			colors: ['#22d3ee', '#a3e635', '#f59e0b', '#fb923c', '#f87171'],
			higherIsWorse: true,
			unit: 'µg/m³',
			note: 'Bandas de PM2.5 (µg/m³, media anual) según metas intermedias OMS 2021: ≤10 · 10–15 · 15–25 · 25–35 · >35 · Clic en un flujo o banda para ver esos hexágonos en el mapa',
		},
		// Regional range covers every territory (7.4–19.2 µg/m³ in pm25_scale.json).
		regionalColorDomain: [7, 20],
		scaleStats: PM25_SCALE,
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
	},
	// ── Deforestation Dynamics (Hansen 2001-2024, observed) ──
	// No petalVars: only 2 numeric indicators (loss_rate + cumulative) — radar with 2 vars is not meaningful.
	deforestation_dynamics: {
		id: 'deforestation_dynamics',
		parquet: 'sat_deforestation_dynamics',
		variables: [
			{ col: 'type', labelKey: 'sat.deforest.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.deforest.typeLabel', aggregation: 'mean' },
			{ col: 'c_loss_rate', unit: '%/yr', labelKey: 'sat.deforest.lossRate', aggregation: 'mean' },
			{ col: 'c_cumulative', unit: '%', labelKey: 'sat.deforest.cumulative', aggregation: 'mean' },
		],
		primaryVariable: 'c_loss_rate',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.deforest.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2015–2024', baseline: '2001–2010', source: 'Hansen GFC v1.12' },
		// Three readings of the same Hansen loss (pipeline/build_deforest_rank.py):
		// where the most is lost, where it is most intense, where it sped up or slowed.
		deptRank: {
			field: 'loss_ha_yr', unit: 'ha/año', unitKey: 'rank.unit.haYr', decimals: 0,
			titleKey: 'rank.deforest.title.ha', noteKey: 'rank.deforest.note.ha',
			metrics: [
				{ tabKey: 'rank.deforest.tab.ha' },
				{ tabKey: 'rank.deforest.tab.pct', field: 'loss_pct_forest', unit: '%/año', unitKey: 'rank.unit.pctYr', decimals: 2,
				  titleKey: 'rank.deforest.title.pct', noteKey: 'rank.deforest.note.pct',
				  extraField: 'loss_ha_yr', extraUnitKey: 'rank.unit.haYr', extraDecimals: 0 },
				{ tabKey: 'rank.deforest.tab.change', field: 'loss_change_pct', unit: '%', unitKey: 'rank.unit.pct', decimals: 0, signed: true,
				  titleKey: 'rank.deforest.title.change', noteKey: 'rank.deforest.note.change',
				  extraField: 'loss_ha_yr', extraUnitKey: 'rank.unit.haYr', extraDecimals: 0 },
			],
		},
		// Bands on RAW loss rate (%/yr, Hansen goalpost [0,5]): higher = worse,
		// so band colors and improved/worsened flow semantics are inverted.
		flowBands: {
			col: 'c_loss_rate',
			baselineCol: 'c_loss_rate_baseline',
			breaks: [0.5, 2],
			labels: ['Baja', 'Media', 'Alta'],
			colors: ['#22d3ee', '#f59e0b', '#f87171'],
			higherIsWorse: true,
			unit: '%/año',
			note: 'Bandas de pérdida forestal (%/año, Hansen): Baja (≤0.5) · Media (0.5–2) · Alta (>2) · Clic en un flujo o banda para ver esos hexágonos en el mapa',
		},
		colorClasses: DEFOREST_LOSS_CLASSES,
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
				{ tabKey: 'rank.deforest.tab.pct', field: 'loss_pct_forest', unit: '%', unitKey: 'rank.unit.pct', decimals: 1,
				  titleKey: 'rank.eudr.title.pct', noteKey: 'rank.eudr.note.pct',
				  extraField: 'loss_ha', extraUnitKey: 'rank.unit.ha', extraDecimals: 0 },
				{ tabKey: 'rank.eudr.tab.fire', field: 'fire_native_ha', unit: 'ha', unitKey: 'rank.unit.ha', decimals: 0,
				  titleKey: 'rank.eudr.title.fire', noteKey: 'rank.eudr.note.fire' },
			],
		},
		legendLowKey: 'legend.eudr.low',
		legendHighKey: 'legend.eudr.high',
		coverage: { corrientes: 'available', chaco: 'available', formosa: 'available', alto_parana_py: 'available', itapua_py: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'unavailable'},
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
	spatialUnit?: 'radio' | 'hexagon' | 'catastro';
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
		comparable: true,
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	// ── Producción y suelo ──
	{
		id: 'agri_potential',
		lensId: 'produccion',
		titleKey: 'sat.agri.title',
		descKey: 'sat.agri.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', cordillera_py: 'available', san_pedro_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available'},
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
		comparable: true,
		rigorBadge: 'physical',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', central_py: 'available', neembucu_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'available', misiones_py: 'available', paraguari_py: 'available', amambay_py: 'available'},
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
	// ── Economía e infraestructura ──
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available', boqueron_py: 'available', alto_paraguay_py: 'unavailable'},
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

// ── Radio-based analysis configurations ─────────────────────────────────────
export type RadioPetalCol = {
	col: string;
	label: { es: string; en: string };
	desc: { es: string; en: string };
	invert: boolean;
	source?: 'catastro_by_radio';
};

export type RadioAnalysisConfig = {
	id: string;
	choroplethCol: string;
	colorScale: 'price' | 'sequential' | 'flood';
	invertChoropleth?: boolean;
	petalCols: RadioPetalCol[];
	howToRead: { es: string; en: string };
	implications: { es: string; en: string };
	methodology: { es: string; en: string };
};

export const RADIO_ANALYSIS_REGISTRY: Record<string, RadioAnalysisConfig> = {
	investment_value: {
		id: 'investment_value',
		choroplethCol: 're_median_usd_m2',
		colorScale: 'price',
		petalCols: [
			{ col: 're_median_usd_m2', label: { es: 'Precio USD/m²', en: 'Price USD/m²' }, desc: { es: 'Mediana de publicaciones activas', en: 'Median of active listings' }, invert: false },
			{ col: 're_n_listings', label: { es: 'Publicaciones', en: 'Listings' }, desc: { es: 'Cantidad de avisos activos en la zona', en: 'Number of active listings in the area' }, invert: false },
			{ col: 'opportunity_score', label: { es: 'Oportunidad', en: 'Opportunity' }, desc: { es: 'Score compuesto de potencial de inversión', en: 'Composite investment potential score' }, invert: false },
			{ col: 're_attract_score', label: { es: 'Atractivo inmob.', en: 'RE Attractiveness' }, desc: { es: 'Score de atractivo para el mercado', en: 'Market attractiveness score' }, invert: false },
			{ col: 'building_density_per_km2', label: { es: 'Densidad edilicia', en: 'Building density' }, desc: { es: 'Edificios por km²', en: 'Buildings per km²' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según el precio mediano USD/m² de la zona. Colores cálidos = mayor precio. El pétalo compara precio, oferta, oportunidad, atractivo y densidad de la zona.', en: 'Parcels are coloured by median USD/m² price. Warmer colours = higher price. The petal compares price, supply, opportunity, attractiveness and density.' },
		implications: { es: 'Zonas con alto precio y pocas publicaciones indican mercado cerrado. Zonas con score de oportunidad alto y precio bajo pueden representar oportunidades de inversión.', en: 'High price with few listings indicates a closed market. High opportunity score with low price may represent investment opportunities.' },
		methodology: { es: 'Precio mediano USD/m² de publicaciones activas (MeLi/Argenprop). Score de oportunidad = precio bajo + accesibilidad alta + consolidación urbana. Cobertura: 26% de radios con datos de mercado.', en: 'Median USD/m² from active listings (MeLi/Argenprop). Opportunity score = low price + high accessibility + urban consolidation. Coverage: 26% of radios with market data.' },
	},
	natural_risks: {
		id: 'natural_risks',
		choroplethCol: 'landslide_score',
		colorScale: 'flood',
		petalCols: [
			{ col: 'flood_frequency', label: { es: 'Inundación', en: 'Flooding' }, desc: { es: 'Frecuencia histórica de inundaciones', en: 'Historical flood frequency' }, invert: false },
			{ col: 'twi_mean', label: { es: 'Humedad topogr.', en: 'Topo. wetness' }, desc: { es: 'Índice de humedad topográfica — proxy de inestabilidad del terreno (SRTM TWI)', en: 'Topographic wetness index — terrain instability proxy (SRTM TWI)' }, invert: false },
			{ col: 'rusle_mean', label: { es: 'Erosión', en: 'Erosion' }, desc: { es: 'Potencial de erosión hídrica (RUSLE)', en: 'Water erosion potential (RUSLE)' }, invert: false },
			{ col: 'slope_mean', label: { es: 'Pendiente', en: 'Slope' }, desc: { es: 'Pendiente media del terreno (%)', en: 'Mean terrain slope (%)' }, invert: false },
			{ col: 'deforest_pressure_score', label: { es: 'Presión deforest.', en: 'Deforestation' }, desc: { es: 'Presión de deforestación en la zona', en: 'Deforestation pressure in the area' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según la susceptibilidad histórica a deslizamientos. Colores cálidos = mayor susceptibilidad. El pétalo muestra 5 métricas de riesgo natural: inundación, humedad topográfica, erosión, pendiente y presión de deforestación.', en: 'Parcels are coloured by historical landslide susceptibility. Warmer colours = higher susceptibility. The petal shows 5 natural risk metrics: flooding, topographic wetness, erosion, slope and deforestation pressure.' },
		implications: { es: 'Parcelas con múltiples riesgos altos requieren estudios de suelo antes de invertir. Pendiente alta + erosión alta señalan terrenos inestables. La deforestación reciente agrava todos los riesgos.', en: 'Parcels with multiple high risks require soil studies before investing. High slope + high erosion indicate unstable terrain.' },
		methodology: { es: 'Índice 0–100: media geométrica de componentes validados por PCA. Indicadores: frecuencia de inundación JRC, susceptibilidad deslizamiento, erosión RUSLE, pendiente FABDEM, presión de deforestación Hansen.', en: 'Index 0–100: geometric mean of PCA-validated components. Indicators: JRC flood frequency, landslide susceptibility, RUSLE erosion, mean slope, deforestation pressure.' },
	},
	productive_aptitude: {
		id: 'productive_aptitude',
		choroplethCol: 'agri_potential_score',
		colorScale: 'sequential',
		petalCols: [
			{ col: 'soil_ph', label: { es: 'pH del suelo', en: 'Soil pH' }, desc: { es: 'Acidez/alcalinidad del suelo (SoilGrids)', en: 'Soil acidity/alkalinity (SoilGrids)' }, invert: false },
			{ col: 'soil_organic_carbon', label: { es: 'Carbono orgánico', en: 'Organic carbon' }, desc: { es: 'Contenido de materia orgánica', en: 'Organic matter content' }, invert: false },
			{ col: 'chirps_annual_mm', label: { es: 'Precipitación', en: 'Rainfall' }, desc: { es: 'Lluvia anual media en mm (CHIRPS)', en: 'Mean annual rainfall in mm (CHIRPS)' }, invert: false },
			{ col: 'slope_mean', label: { es: 'Pendiente', en: 'Slope' }, desc: { es: 'Pendiente media — menor es mejor para cultivos', en: 'Mean slope — lower is better for crops' }, invert: true },
			{ col: 'agri_potential_score', label: { es: 'Aptitud agrícola', en: 'Ag. potential' }, desc: { es: 'Score compuesto de potencial productivo', en: 'Composite productive potential score' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según el score de aptitud agrícola. Verde más intenso = mayor potencial. El pétalo muestra suelo, lluvia, pendiente y aptitud general.', en: 'Parcels are coloured by agricultural aptitude score. Greener = higher potential.' },
		implications: { es: 'Suelos con pH 5.5-6.5 y alto carbono orgánico son óptimos para yerba mate y té. Pendientes >15% requieren terrazas. Precipitación >1600mm/año permite cultivos sin riego.', en: 'Soils with pH 5.5-6.5 and high organic carbon are optimal for yerba mate and tea.' },
		methodology: { es: 'Índice 0–100: media geométrica de componentes validados por PCA. Indicadores: pH del suelo SoilGrids (óptimo 5.5–6.5), carbono orgánico, precipitación CHIRPS, pendiente FABDEM. Fuentes: SoilGrids 250m + CHIRPS 5km.', en: 'Index 0–100: geometric mean of PCA-validated components. Indicators: soil pH SoilGrids, organic carbon, CHIRPS precipitation, FABDEM slope.' },
	},
	accessibility: {
		id: 'accessibility',
		choroplethCol: 'travel_min_capital',
		colorScale: 'sequential',
		invertChoropleth: true,
		petalCols: [
			{ col: 'travel_min_capital', label: { es: 'Tiempo a capital', en: 'Time to capital' }, desc: { es: 'Minutos de viaje a la capital provincial', en: 'Travel minutes to the provincial capital' }, invert: true },
			{ col: 'travel_min_cabecera', label: { es: 'Tiempo a cabecera', en: 'Time to dept. seat' }, desc: { es: 'Minutos a la cabecera departamental', en: 'Minutes to department seat' }, invert: true },
			{ col: 'dist_nearest_hospital_km', label: { es: 'Hospital', en: 'Hospital' }, desc: { es: 'Distancia al hospital más cercano (km)', en: 'Distance to nearest hospital (km)' }, invert: true },
			{ col: 'dist_nearest_secundaria_km', label: { es: 'Escuela sec.', en: 'High school' }, desc: { es: 'Distancia a escuela secundaria (km)', en: 'Distance to high school (km)' }, invert: true },
			{ col: 'dist_primary_m', label: { es: 'Ruta principal', en: 'Main road' }, desc: { es: 'Distancia a ruta primaria (km)', en: 'Distance to primary road (km)' }, invert: true },
		],
		howToRead: { es: 'Las parcelas se colorean según el tiempo de viaje a la capital. Verde = más cercano. El pétalo muestra accesibilidad a servicios clave — en este caso mayor extensión = mejor acceso (menor distancia/tiempo).', en: 'Parcels are coloured by travel time to the capital. Green = closer. The petal shows accessibility to key services.' },
		implications: { es: 'Zonas a más de 2 horas de la capital y sin hospital cercano representan alto riesgo para familias. La distancia a ruta principal determina el costo logístico para producción.', en: 'Areas over 2 hours from the capital without a nearby hospital represent high risk for families.' },
		methodology: { es: 'Tiempo de viaje motorizado a la capital y cabecera departamental (superficie de fricción Oxford MAP). Distancia a hospitales, escuelas y rutas principales (OSM). Fuente: Nelson et al. 2019 + Oxford MAP 2019 + OSM.', en: 'Motorised travel time to capital and dept seat (Oxford MAP friction surface). Distance to hospitals, schools and primary roads (OSM).' },
	},
	change_dynamics: {
		id: 'change_dynamics',
		choroplethCol: 'deforest_pressure_score',
		colorScale: 'sequential',
		petalCols: [
			{ col: 'n_new_parcels_90d', label: { es: 'Parcelas nuevas', en: 'New parcels' }, desc: { es: 'Parcelas creadas en últimos 90 días', en: 'Parcels created in last 90 days' }, invert: false, source: 'catastro_by_radio' },
			{ col: 'deforest_pressure_score', label: { es: 'Presión deforest.', en: 'Deforestation' }, desc: { es: 'Intensidad de deforestación reciente', en: 'Recent deforestation intensity' }, invert: false },
			{ col: 'building_density_per_km2', label: { es: 'Densidad edilicia', en: 'Building density' }, desc: { es: 'Edificios por km² (proxy presión urbana)', en: 'Buildings per km² (urban pressure proxy)' }, invert: false },
			{ col: 'mb_urban_frac', label: { es: 'Fracción urbana', en: 'Urban fraction' }, desc: { es: '% de superficie urbanizada', en: '% urbanised surface' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según la presión de deforestación. Mayor intensidad de color = mayor cambio. El pétalo muestra 4 indicadores de dinámica geoespacial.', en: 'Parcels are coloured by deforestation pressure. Higher intensity = more change.' },
		implications: { es: 'Zonas con muchas parcelas nuevas y alta presión de deforestación están en transición activa — posible valorización pero también riesgo ambiental. Alta densidad edilicia + baja fracción urbana indica expansión informal.', en: 'Areas with many new parcels and high deforestation pressure are in active transition.' },
		methodology: { es: 'Parcelas nuevas: Catastro Misiones (ventana 90 días). Deforestación: Hansen GFC pérdida acumulada. Densidad edilicia: Global Building Atlas. Fracción urbana: MapBiomas Argentina.', en: 'New parcels: Misiones Cadastre (90-day window). Deforestation: Hansen GFC cumulative loss. Building density: GBA. Urban fraction: MapBiomas.' },
	},
	sociodemographic: {
		id: 'sociodemographic',
		choroplethCol: 'pct_nbi',
		colorScale: 'flood',
		petalCols: [
			{ col: 'densidad_hab_km2', label: { es: 'Densidad hab.', en: 'Pop. density' }, desc: { es: 'Habitantes por km²', en: 'Inhabitants per km²' }, invert: false },
			{ col: 'pct_nbi', label: { es: 'NBI', en: 'UBN' }, desc: { es: '% hogares con necesidades básicas insatisfechas', en: '% households with unmet basic needs' }, invert: false },
			{ col: 'pct_hacinamiento', label: { es: 'Hacinamiento', en: 'Overcrowding' }, desc: { es: '% hogares en condiciones de hacinamiento', en: '% households in overcrowded conditions' }, invert: false },
			{ col: 'pct_propietario', label: { es: 'Propietarios', en: 'Homeowners' }, desc: { es: '% hogares propietarios de la vivienda', en: '% homeowner households' }, invert: false },
			{ col: 'tamano_medio_hogar', label: { es: 'Tamaño hogar', en: 'Household size' }, desc: { es: 'Personas promedio por hogar', en: 'Average persons per household' }, invert: false },
			{ col: 'pct_computadora', label: { es: 'Computadora', en: 'Computer' }, desc: { es: '% hogares con computadora', en: '% households with computer' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según el % de NBI (necesidades básicas insatisfechas). Colores cálidos = mayor pobreza. El pétalo muestra el perfil sociodemográfico de la zona: densidad, NBI, hacinamiento, propiedad, tamaño de hogar y acceso digital.', en: 'Parcels are coloured by UBN %. Warmer = higher poverty. The petal shows the sociodemographic profile.' },
		implications: { es: 'Zonas con alto NBI y hacinamiento requieren políticas de vivienda social. Alto % de propietarios indica estabilidad residencial. Baja conectividad digital limita servicios y educación remota.', en: 'High UBN and overcrowding require social housing policies. High homeownership indicates residential stability.' },
		methodology: { es: 'Fuente: INDEC Censo Nacional de Población, Hogares y Viviendas 2022. Variables a nivel radio censal (2,012 radios). NBI según metodología INDEC. Densidad calculada como personas/km².', en: 'Source: INDEC National Population Census 2022. Variables at radio censal level (2,012 radios). UBN per INDEC methodology.' },
	},
	forest_potential: {
		id: 'forest_potential',
		choroplethCol: 'canopy_cover',
		colorScale: 'sequential',
		petalCols: [
			{ col: 'canopy_cover', label: { es: 'Cobertura dosel', en: 'Canopy cover' }, desc: { es: 'Fracción de suelo cubierto por dosel arbóreo', en: 'Fraction of ground covered by tree canopy' }, invert: false },
			{ col: 'canopy_height_mean', label: { es: 'Altura dosel', en: 'Canopy height' }, desc: { es: 'Altura media del dosel en metros (GEDI)', en: 'Mean canopy height in meters (GEDI)' }, invert: false },
			{ col: 'ndvi_mean', label: { es: 'NDVI', en: 'NDVI' }, desc: { es: 'Índice de vegetación (0-1, mayor = más verde)', en: 'Vegetation index (0-1, higher = greener)' }, invert: false },
			{ col: 'frac_bosque_nativo', label: { es: 'Bosque nativo', en: 'Native forest' }, desc: { es: 'Fracción de bosque nativo remanente', en: 'Remaining native forest fraction' }, invert: false },
			{ col: 'treecover2000', label: { es: 'Cobertura 2000', en: 'Treecover 2000' }, desc: { es: '% de cobertura arbórea en el año 2000 (Hansen)', en: '% tree cover in year 2000 (Hansen)' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según la cobertura de dosel arbóreo. Verde más intenso = mayor cobertura. El pétalo muestra 5 métricas de vegetación: cobertura, altura, verdor, bosque nativo y cobertura histórica.', en: 'Parcels are coloured by canopy cover. Greener = more cover. The petal shows 5 vegetation metrics.' },
		implications: { es: 'Alta cobertura de dosel + bosque nativo alto indica zonas de conservación prioritaria. Baja cobertura con alto treecover2000 sugiere deforestación reciente. NDVI alto con baja altura de dosel puede indicar plantaciones vs bosque nativo.', en: 'High canopy cover + native forest indicates priority conservation areas.' },
		methodology: { es: 'Cobertura de dosel: Meta/WRI Canopy Height v2 (1m agregado a 10m). NDVI: MODIS 250m promedio 2019-2024. Bosque nativo: MapBiomas Argentina. Cobertura histórica: Hansen GFC baseline 2000.', en: 'Canopy cover: Meta/WRI CHM v2. NDVI: MODIS 250m mean 2019-2024. Native forest: MapBiomas. Historical cover: Hansen GFC 2000.' },
	},
	economic_activity: {
		id: 'economic_activity',
		choroplethCol: 'tasa_empleo',
		colorScale: 'sequential',
		petalCols: [
			{ col: 'tasa_empleo', label: { es: 'Empleo', en: 'Employment' }, desc: { es: 'Tasa de empleo (% población ocupada)', en: 'Employment rate (% employed population)' }, invert: false },
			{ col: 'tasa_actividad', label: { es: 'Actividad', en: 'Activity' }, desc: { es: 'Tasa de actividad económica', en: 'Economic activity rate' }, invert: false },
			{ col: 'pct_universitario', label: { es: 'Universitarios', en: 'University' }, desc: { es: '% población con título universitario', en: '% population with university degree' }, invert: false },
			{ col: 'viirs_mean_radiance', label: { es: 'Luces nocturnas', en: 'Night lights' }, desc: { es: 'Radiancia media nocturna (proxy actividad económica)', en: 'Mean night-time radiance (economic activity proxy)' }, invert: false },
			{ col: 'building_density_per_km2', label: { es: 'Densidad edilicia', en: 'Building density' }, desc: { es: 'Edificios por km²', en: 'Buildings per km²' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según la tasa de empleo. Verde más intenso = mayor empleo. El pétalo combina empleo, actividad económica, formación universitaria, luces nocturnas y densidad edilicia como indicadores de dinamismo.', en: 'Parcels are coloured by employment rate. Greener = higher employment.' },
		implications: { es: 'Zonas con alta tasa de empleo + universitarios + luces nocturnas son centros económicos consolidados. Zonas con alta actividad pero bajo empleo pueden tener alta informalidad. Baja radiancia nocturna indica zonas rurales o de baja actividad.', en: 'High employment + university + night lights indicate consolidated economic centres.' },
		methodology: { es: 'Empleo y actividad: Censo 2022 INDEC (población 14+ años). Universitarios: Censo 2022. Luces nocturnas: VIIRS 500m promedio 2022-2024. Densidad edilicia: Global Building Atlas 2025.', en: 'Employment and activity: INDEC Census 2022 (14+ population). Night lights: VIIRS 500m mean 2022-2024. Building density: GBA 2025.' },
	},
};

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
	real_estate: {
		dataDate: '01/2025',
		processedDate: '21/03/2026',
		sourceKey: 'data.source.realEstate',
	},
	catastro_by_radio: {
		dataDate: 'marzo 2026',
		processedDate: '22/03/2026',
		sourceKey: 'data.source.catastro',
	},
	buildings_stats: {
		dataDate: '02/2025',
		processedDate: '21/03/2026',
		sourceKey: 'data.source.buildings',
	},
	overture_buildings: {
		dataDate: 'Overture 2026-03-18',
		processedDate: '24/03/2026',
		sourceKey: 'data.source.overture',
	},
	overture_transportation: {
		dataDate: 'Overture 2026-03-18',
		processedDate: '24/03/2026',
		sourceKey: 'data.source.overture',
	},
	overture_places: {
		dataDate: 'Overture 2026-03-18',
		processedDate: '24/03/2026',
		sourceKey: 'data.source.overture',
	},
	overture_base: {
		dataDate: 'Overture 2026-03-18',
		processedDate: '24/03/2026',
		sourceKey: 'data.source.overture',
	},
	sat_agri_potential: { dataDate: 'Baseline SoilGrids / CHIRPS / ERA5 2019-2024', processedDate: '20/04/2026', sourceKey: 'data.source.satellite' },
	sat_forestry_aptitude: { dataDate: 'ERA5/CHIRPS/SoilGrids/SRTM 2019-2023', processedDate: '21/04/2026', sourceKey: 'data.source.satellite' },
	sat_service_deprivation: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '02/04/2026', sourceKey: 'data.source.censo' },
	sat_health_access: { dataDate: 'Oxford MAP 2019 + Censo 2022', processedDate: '02/04/2026', sourceKey: 'data.source.satellite' },
	sat_education_capital: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '02/04/2026', sourceKey: 'data.source.censo' },
	sat_education_flow: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '02/04/2026', sourceKey: 'data.source.censo' },
	sat_sociodemographic: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '29/03/2026', sourceKey: 'data.source.censo' },
	sat_censo_temporal: { dataDate: 'Censos INDEC 1991 · 2001 · 2010 · 2022', processedDate: '10/06/2026', sourceKey: 'data.source.censo' },
	sat_economic_activity: { dataDate: 'Censo 2022 + VIIRS 2022-2024 + GBA 2025', processedDate: '29/03/2026', sourceKey: 'data.source.satellite' },
	sat_accessibility: { dataDate: 'Nelson 2019 / Oxford MAP 2019 / OSM', processedDate: '30/05/2026', sourceKey: 'data.source.satellite' },
	// EUDR: los parquets por depto salen del grid res-9 (build_eudr_dpto.py, 30/10/2026); el refresh mensual del grid está en data/eudr/meta.json.
	sat_eudr: { dataDate: 'Hansen GFC v1.13 (2021-2025) + MODIS MCD64A1 x MapBiomas Col.2 (cutoff 31/12/2020)', processedDate: '24/07/2026', sourceKey: 'data.source.satellite' },
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
