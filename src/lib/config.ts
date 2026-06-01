const R2_PROD = 'https://pub-580c676bec7f4eeb96d7d30559a3cab7.r2.dev';

function getBase(): string {
	return R2_PROD;
}

export function getTilesUrl(name: 'buildings' | 'itapua_buildings' | 'itapua_districts' | 'corrientes_buildings' | 'alto_parana_buildings' | 'alto_parana_districts' | 'chaco_buildings' | 'formosa_buildings' | 'parana_br_buildings' | 'santa_catarina_br_buildings' | 'rio_grande_sul_br_buildings' | 'radios' | 'terrain' | 'hexagons' | 'catastro' | 'concepcion_py_buildings' | 'concepcion_py_districts' | 'san_pedro_py_buildings' | 'san_pedro_py_districts' | 'cordillera_py_buildings' | 'cordillera_py_districts' | 'guaira_py_buildings' | 'guaira_py_districts' | 'caaguazu_py_buildings' | 'caaguazu_py_districts' | 'caazapa_py_buildings' | 'caazapa_py_districts' | 'misiones_py_buildings' | 'misiones_py_districts' | 'paraguari_py_buildings' | 'paraguari_py_districts' | 'central_py_buildings' | 'central_py_districts' | 'neembucu_py_buildings' | 'neembucu_py_districts' | 'amambay_py_buildings' | 'amambay_py_districts' | 'canindeyu_py_buildings' | 'canindeyu_py_districts' | 'presidente_hayes_py_buildings' | 'presidente_hayes_py_districts' | 'boqueron_py_buildings' | 'boqueron_py_districts' | 'alto_paraguay_py_buildings' | 'alto_paraguay_py_districts'): string {
	if (name === 'terrain') {
		return '/api/terrain/{z}/{x}/{y}.png';
	}
	const files: Record<string, string> = {
		buildings: 'tiles/buildings-v5.pmtiles',
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
		hex_flood_risk: '?v=36',
		sat_agri_potential: '?v=35',
		sat_forestry_aptitude: '?v=35',
		sat_service_deprivation: '?v=27',
		sat_health_access: '?v=27',
		sat_education_capital: '?v=27',
		sat_education_flow: '?v=27',
		sat_sociodemographic: '?v=24',
		sat_economic_activity: '?v=24',
		sat_accessibility: '?v=27',
		sat_carbon_stock: '?v=20',
		sat_pm25_drivers: '?v=17',
		sat_deforestation_dynamics: '?v=16',
		sat_land_use: '?v=4',
		emsa_powerlines: '?v=20',
	};
	const bust = busts[name] || '';
	return `${getBase()}/data/${name}.parquet${bust}`;
}

// Bump DEPT_V after any pipeline run that regenerates dept-split parquets.
// Cloudflare CDN caches stable versioned URLs; a new version invalidates naturally.
const DEPT_V = 1;

export function getFloodDptoUrl(parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}flood_dpto/hex_flood_${parquetKey}.parquet?v=${DEPT_V}`;
}

export function getScoresDptoUrl(parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}scores_dpto/overture_scores_${parquetKey}.parquet?v=${DEPT_V}`;
}

export function getSatDptoUrl(analysisId: string, parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}sat_dpto/sat_${analysisId}_${parquetKey}.parquet?v=${DEPT_V}`;
}

export function getReportUrl(analysisId: string, parquetKey: string, territoryPrefix = ''): string {
	return `${getBase()}/data/${territoryPrefix}reports/sat_${analysisId}_${parquetKey}.pdf`;
}

// EUDR hi-res (res-9) deforestation grid — single combined parquet, sorted by
// h3index with row-group stats so DuckDB-WASM range-reads only the matching group.
export function getEudrHiresUrl(): string {
	return `${getBase()}/data/eudr/hires/eudr_res9_combined.parquet?v=1`;
}

export function getSatGlobalUrl(analysisId: string, territoryPrefix = ''): string {
	const layer = HEX_LAYER_REGISTRY[analysisId];
	const name = layer?.parquet ?? `sat_${analysisId}`;
	const base = getParquetUrl(name); // inherits static ?v=N from busts map
	if (!territoryPrefix) return base;
	return base.replace('/data/', `/data/${territoryPrefix}`);
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
	return `${getBase()}/data/br_districts/${territoryId}_districts.geojson`;
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
	get sat_carbon_stock() { return getParquetUrl('sat_carbon_stock'); },
	// Brazil IBGE Censo 2022 setor-level stats
	get radio_stats_parana_br()         { return `${getBase()}/data/setores_stats_parana_br.parquet`; },
	get radio_stats_santa_catarina_br() { return `${getBase()}/data/setores_stats_santa_catarina_br.parquet`; },
	get radio_stats_rio_grande_sul_br() { return `${getBase()}/data/setores_stats_rio_grande_sul_br.parquet`; },
	// Public infrastructure (datos.gob.ar)
	get emsa_powerlines() { return getParquetUrl('emsa_powerlines'); },
	// EUDR deforestation (H3 res-7, 10 provinces)
	get eudr_deforestation() { return getEudrParquetUrl('eudr_deforestation'); },
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
		parquetPrefix: 'boqueron_py/', flag: '🇵🇾', available: false,
	},
	alto_paraguay_py: {
		id: 'alto_paraguay_py', label: 'Alto Paraguay', shortLabel: 'AGP', country: 'py',
		bbox: [-61.2, -23.2, -57.4, -18.4],
		parquetPrefix: 'alto_paraguay_py/', flag: '🇵🇾', available: false,
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
	hideIfZero?: boolean;
}

export type TemporalMode = 'current' | 'baseline' | 'delta';

export interface HexLayerConfig {
	id: string;
	parquet: string;
	variables: HexVariable[];
	primaryVariable: string;
	colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm';
	aggregation: 'mean' | 'sum' | 'max';
	petalVars?: HexVariable[];
	titleKey: string;
	perDepartment?: boolean;
	temporal?: boolean;
	temporalPeriods?: { current: string; baseline: string; source?: string };
	coverage?: Record<string, 'available' | 'pending' | 'unavailable'>;
	legendLowKey?: string;
	legendHighKey?: string;
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
			{ col: 'flood_risk_score', labelKey: 'analysis.flood.riskScore', aggregation: 'mean' },
			{ col: 'type', labelKey: 'analysis.flood.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'analysis.flood.typeLabel', aggregation: 'mean' },
			{ col: 'jrc_occurrence', labelKey: 'analysis.flood.jrcOccurrence', aggregation: 'mean', unit: '%' },
			{ col: 'jrc_recurrence', labelKey: 'analysis.flood.jrcRecurrence', aggregation: 'mean', unit: '%' },
			{ col: 'jrc_seasonality', labelKey: 'analysis.flood.jrcSeasonality', aggregation: 'mean', unit: 'meses' },
			{ col: 'flood_extent_pct', labelKey: 'analysis.flood.currentExtent', aggregation: 'mean', unit: '%' },
		],
		primaryVariable: 'flood_risk_score',
		colorScale: 'flood',
		aggregation: 'mean',
		petalVars: [
			{ col: 'flood_risk_score', labelKey: 'analysis.flood.riskScore', aggregation: 'mean' },
			{ col: 'jrc_occurrence', labelKey: 'analysis.flood.jrcOccurrence', aggregation: 'mean' },
			{ col: 'jrc_recurrence', labelKey: 'analysis.flood.jrcRecurrence', aggregation: 'mean' },
			{ col: 'flood_extent_pct', labelKey: 'analysis.flood.currentExtent', aggregation: 'mean' },
		],
		titleKey: 'analysis.floodRisk.title',
		perDepartment: true,
		legendLowKey: 'legend.flood.low',
		legendHighKey: 'legend.flood.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
	},
	// ── EMSA: Infraestructura eléctrica ──
	powerline_density: {
		id: 'powerline_density',
		parquet: 'emsa_powerlines',
		variables: [
			{ col: 'score', labelKey: 'emsa.score', aggregation: 'mean' },
			{ col: 'line_length_m', labelKey: 'emsa.lineLength', aggregation: 'sum', unit: 'm' },
			{ col: 'line_count', labelKey: 'emsa.lineCount', aggregation: 'sum', unit: 'líneas' },
		],
		primaryVariable: 'score',
		colorScale: 'warm',
		aggregation: 'mean',
		petalVars: [
			{ col: 'score', labelKey: 'emsa.score', aggregation: 'mean' },
			{ col: 'line_length_m', labelKey: 'emsa.lineLength', aggregation: 'sum', unit: 'm' },
			{ col: 'line_count', labelKey: 'emsa.lineCount', aggregation: 'sum', unit: 'líneas' },
		],
		titleKey: 'emsa.title',
		perDepartment: false,
		legendLowKey: 'legend.powerline.low',
		legendHighKey: 'legend.powerline.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'unavailable'},
	},
	// ── Satellite composite scores ──
	agri_potential: {
		id: 'agri_potential',
		parquet: 'sat_agri_potential',
		variables: [
			{ col: 'type', labelKey: 'sat.agri.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.agri.typeLabel', aggregation: 'mean' },
			{ col: 'c_soc', labelKey: 'sat.agri.soc', aggregation: 'mean', unit: '/100' },
			{ col: 'c_ph_optimal', labelKey: 'sat.agri.phOptimal', aggregation: 'mean', unit: '/100' },
			{ col: 'c_clay', labelKey: 'sat.agri.clay', aggregation: 'mean', unit: '/100' },
			{ col: 'c_precipitation', labelKey: 'sat.agri.precip', aggregation: 'mean', unit: '/100' },
			{ col: 'c_gdd', labelKey: 'sat.agri.gdd', aggregation: 'mean', unit: '/100' },
			{ col: 'c_slope', labelKey: 'sat.agri.slope', aggregation: 'mean', unit: '/100' },
		],
		primaryVariable: 'score',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.agri.title',
		perDepartment: true,
		legendLowKey: 'legend.agri.low',
		legendHighKey: 'legend.agri.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available'},
	},
	forestry_aptitude: {
		id: 'forestry_aptitude',
		parquet: 'sat_forestry_aptitude',
		variables: [
			{ col: 'type_label', labelKey: 'sat.forestry.typeLabel', aggregation: 'mean' },
			{ col: 'gdd', labelKey: 'sat.forestry.gdd', aggregation: 'mean', unit: '°C·d' },
			{ col: 'precip_total', labelKey: 'sat.forestry.precip', aggregation: 'mean', unit: 'mm' },
			{ col: 'water_deficit', labelKey: 'sat.forestry.waterDeficit', aggregation: 'mean', unit: 'mm' },
			{ col: 'slope_mean', labelKey: 'sat.forestry.slope', aggregation: 'mean', unit: '°' },
			{ col: 'clay', labelKey: 'sat.forestry.clay', aggregation: 'mean', unit: '%' },
			{ col: 'soc', labelKey: 'sat.forestry.soc', aggregation: 'mean', unit: 'g/kg' },
		],
		primaryVariable: 'score',
		colorScale: 'green',
		aggregation: 'mean',
		titleKey: 'sat.forestry.title',
		perDepartment: true,
		legendLowKey: 'legend.forestry.low',
		legendHighKey: 'legend.forestry.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
	},
	service_deprivation: {
		id: 'service_deprivation',
		parquet: 'sat_service_deprivation',
		variables: [
			{ col: 'type', labelKey: 'sat.deprivation.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.deprivation.typeLabel', aggregation: 'mean' },
			{ col: 'c_nbi', labelKey: 'sat.deprivation.nbi', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_sin_cloacas', labelKey: 'sat.deprivation.sinCloacas', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_piso', labelKey: 'sat.deprivation.piso', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_hacinamiento', labelKey: 'sat.deprivation.hacinamiento', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_hacinamiento_crit', labelKey: 'sat.deprivation.hacinamientoCrit', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_sin_computadora', labelKey: 'sat.deprivation.sinComputadora', aggregation: 'mean', unit: 'percentil' },
		],
		primaryVariable: 'score',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.deprivation.title',
		perDepartment: true,
		legendLowKey: 'legend.deprivation.low',
		legendHighKey: 'legend.deprivation.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	health_access: {
		id: 'health_access',
		parquet: 'sat_health_access',
		variables: [
			{ col: 'type', labelKey: 'sat.health.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.health.typeLabel', aggregation: 'mean' },
			{ col: 'c_healthcare_time', labelKey: 'sat.health.time', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_health_coverage', labelKey: 'sat.health.coverage', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_nbi', labelKey: 'sat.health.nbi', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_elderly', labelKey: 'sat.health.elderly', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_children', labelKey: 'sat.health.children', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_pop_density', labelKey: 'sat.health.popDensity', aggregation: 'mean', unit: 'percentil' },
		],
		primaryVariable: 'score',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.health.title',
		perDepartment: true,
		legendLowKey: 'legend.health.low',
		legendHighKey: 'legend.health.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	education_capital: {
		id: 'education_capital',
		parquet: 'sat_education_capital',
		variables: [
			{ col: 'type', labelKey: 'sat.eduCap.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.eduCap.typeLabel', aggregation: 'mean' },
			{ col: 'c_no_schooling', labelKey: 'sat.eduCap.noSchooling', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_secondary_plus', labelKey: 'sat.eduCap.secondaryPlus', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_higher_edu', labelKey: 'sat.eduCap.higherEdu', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_university', labelKey: 'sat.eduCap.university', aggregation: 'mean', unit: 'percentil' },
		],
		primaryVariable: 'score',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.eduCap.title',
		perDepartment: true,
		legendLowKey: 'legend.eduCap.low',
		legendHighKey: 'legend.eduCap.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	education_flow: {
		id: 'education_flow',
		parquet: 'sat_education_flow',
		variables: [
			{ col: 'type', labelKey: 'sat.eduFlow.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.eduFlow.typeLabel', aggregation: 'mean' },
			{ col: 'c_dropout_primary', labelKey: 'sat.eduFlow.dropoutPrimary', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_dropout_secondary', labelKey: 'sat.eduFlow.dropoutSecondary', aggregation: 'mean', unit: 'percentil' },
			{ col: 'c_teen_pregnancy', labelKey: 'sat.eduFlow.teenPregnancy', aggregation: 'mean', unit: 'percentil' },
		],
		primaryVariable: 'score',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'sat.eduFlow.title',
		perDepartment: true,
		legendLowKey: 'legend.eduFlow.low',
		legendHighKey: 'legend.eduFlow.high',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	// ── Land use / MapBiomas ──
	land_use: {
		id: 'land_use',
		parquet: 'sat_land_use',
		variables: [
			{ col: 'score',              labelKey: 'sat.landUse.score',        aggregation: 'mean' },
			{ col: 'type',               labelKey: 'sat.landUse.type',         aggregation: 'mean' },
			{ col: 'type_label',         labelKey: 'sat.landUse.typeLabel',    aggregation: 'mean' },
			{ col: 'frac_native_forest', labelKey: 'sat.landUse.nativeForest', aggregation: 'mean' },
			{ col: 'frac_plantation',    labelKey: 'sat.landUse.plantation',   aggregation: 'mean' },
			{ col: 'frac_agriculture',   labelKey: 'sat.landUse.agriculture',  aggregation: 'mean' },
			{ col: 'frac_pasture',       labelKey: 'sat.landUse.pasture',      aggregation: 'mean' },
			{ col: 'frac_grassland',     labelKey: 'sat.landUse.grassland',    aggregation: 'mean' },
			{ col: 'frac_wetland',       labelKey: 'sat.landUse.wetland',      aggregation: 'mean' },
			{ col: 'frac_urban',         labelKey: 'sat.landUse.urban',        aggregation: 'mean' },
			{ col: 'frac_water',         labelKey: 'sat.landUse.water',        aggregation: 'mean' },
			{ col: 'frac_mosaic',        labelKey: 'sat.landUse.mosaic',       aggregation: 'mean' },
			{ col: 'frac_bare',          labelKey: 'sat.landUse.bare',         aggregation: 'mean' },
		],
		primaryVariable: 'score',
		colorScale: 'sequential',
		aggregation: 'mean',
		titleKey: 'sat.landUse.title',
		perDepartment: true,
		legendLowKey: 'legend.landUse.low',
		legendHighKey: 'legend.landUse.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
	},
	// ── Migrated from radio/catastro to H3 ──
	sociodemographic: {
		id: 'sociodemographic',
		parquet: 'sat_sociodemographic',
		variables: [
			{ col: 'score', labelKey: 'analysis.socio.score', aggregation: 'mean' },
			{ col: 'type', labelKey: 'analysis.socio.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'analysis.socio.typeLabel', aggregation: 'mean' },
			{ col: 'densidad_hab_km2',   labelKey: 'radio.densidad.pctl',    aggregation: 'mean', unit: 'pctl' },
			{ col: 'pct_nbi',            labelKey: 'radio.nbi.pctl',          aggregation: 'mean', unit: 'pctl' },
			{ col: 'pct_hacinamiento',   labelKey: 'radio.hacinamiento.pctl', aggregation: 'mean', unit: 'pctl' },
			{ col: 'pct_propietario',    labelKey: 'radio.propietario.pctl',  aggregation: 'mean', unit: 'pctl' },
			{ col: 'tamano_medio_hogar', labelKey: 'radio.tamHogar.pctl',     aggregation: 'mean', unit: 'pctl' },
			{ col: 'pct_computadora',    labelKey: 'radio.computadora.pctl',  aggregation: 'mean', unit: 'pctl' },
		],
		primaryVariable: 'score',
		colorScale: 'flood',
		aggregation: 'mean',
		titleKey: 'analysis.socio.title',
		perDepartment: true,
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	economic_activity: {
		id: 'economic_activity',
		parquet: 'sat_economic_activity',
		variables: [
			{ col: 'score', labelKey: 'analysis.economic.score', aggregation: 'mean' },
			{ col: 'type', labelKey: 'analysis.economic.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'analysis.economic.typeLabel', aggregation: 'mean' },
			{ col: 'tasa_empleo', labelKey: 'radio.empleo', aggregation: 'mean', unit: '%' },
			{ col: 'tasa_actividad', labelKey: 'radio.actividad', aggregation: 'mean', unit: '%' },
			{ col: 'pct_universitario', labelKey: 'radio.universitario', aggregation: 'mean', unit: '%' },
			{ col: 'viirs_mean_radiance', labelKey: 'radio.viirs', aggregation: 'mean', unit: 'nW/cm²/sr' },
			{ col: 'building_density_per_km2', labelKey: 'radio.buildingDensity', aggregation: 'mean', unit: 'edif/km²' },
		],
		primaryVariable: 'score',
		colorScale: 'warm',
		aggregation: 'mean',
		titleKey: 'analysis.economic.title',
		perDepartment: true,
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available'},
	},
	accessibility: {
		id: 'accessibility',
		parquet: 'sat_accessibility',
		variables: [
			{ col: 'score', labelKey: 'analysis.accessibility.score', aggregation: 'mean' },
			{ col: 'type', labelKey: 'analysis.accessibility.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'analysis.accessibility.typeLabel', aggregation: 'mean' },
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
		comparable: true,
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
	},
	// ── Carbon Stock & Balance ──
	carbon_stock: {
		id: 'carbon_stock',
		parquet: 'sat_carbon_stock',
		variables: [
			{ col: 'type', labelKey: 'sat.carbon.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.carbon.typeLabel', aggregation: 'mean' },
			{ col: 'c_agb_cci', rawCol: 'c_agb_raw', unit: 'Mg/ha', labelKey: 'sat.carbon.agbCci', aggregation: 'mean' },
			{ col: 'c_agb_gedi', rawCol: 'c_agb_gedi_raw', unit: 'Mg/ha', labelKey: 'sat.carbon.agbGedi', aggregation: 'mean' },
			{ col: 'c_total_carbon', rawCol: 'c_total_carbon_raw', unit: 'tC/ha', labelKey: 'sat.carbon.totalCarbon', aggregation: 'mean' },
			{ col: 'c_soc', rawCol: 'c_soc_tcha', unit: 'tC/ha', labelKey: 'sat.carbon.soc', aggregation: 'mean' },
			{ col: 'c_gross_emissions', rawCol: 'c_emissions_raw', unit: 'MgCO₂/ha', labelKey: 'sat.carbon.emissions', aggregation: 'mean' },
			{ col: 'c_gross_removals', rawCol: 'c_removals_raw', unit: 'MgCO₂/ha', labelKey: 'sat.carbon.removals', aggregation: 'mean' },
			{ col: 'c_net_flux', rawCol: 'c_net_flux_raw', unit: 'MgCO₂/ha', labelKey: 'sat.carbon.netFlux', aggregation: 'mean' },
			{ col: 'c_npp', rawCol: 'c_npp_raw', unit: 'gC/m²/yr', labelKey: 'sat.carbon.npp', aggregation: 'mean' },
			{ col: 'c_economic_value', labelKey: 'sat.carbon.economicValue', aggregation: 'mean' },
		],
		primaryVariable: 'c_total_carbon_raw',
		colorScale: 'sequential',
		aggregation: 'mean',
		petalVars: [
			{ col: 'c_agb_cci', labelKey: 'sat.carbon.agbCci', aggregation: 'mean' },
			{ col: 'c_total_carbon', labelKey: 'sat.carbon.totalCarbon', aggregation: 'mean' },
			{ col: 'c_soc', labelKey: 'sat.carbon.soc', aggregation: 'mean' },
			{ col: 'c_gross_emissions', labelKey: 'sat.carbon.emissions', aggregation: 'mean' },
			{ col: 'c_gross_removals', labelKey: 'sat.carbon.removals', aggregation: 'mean' },
			{ col: 'c_net_flux', labelKey: 'sat.carbon.netFlux', aggregation: 'mean' },
			{ col: 'c_npp', labelKey: 'sat.carbon.npp', aggregation: 'mean' },
		],
		titleKey: 'sat.carbon.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2022–2024', baseline: '2018–2020', source: 'ESA CCI · MODIS · Hansen' },
		legendLowKey: 'legend.carbon.low',
		legendHighKey: 'legend.carbon.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available'},
	},
	// ── PM2.5 Drivers (25-year ML decomposition) ──
	pm25_drivers: {
		id: 'pm25_drivers',
		parquet: 'sat_pm25_drivers',
		variables: [
			{ col: 'type', labelKey: 'sat.pm25.type', aggregation: 'mean' },
			{ col: 'type_label', labelKey: 'sat.pm25.typeLabel', aggregation: 'mean' },
			{ col: 'c_pm25_mean', unit: 'µg/m³', labelKey: 'sat.pm25.pm25Mean', aggregation: 'mean' },
			{ col: 'c_fire', labelKey: 'sat.pm25.fire', aggregation: 'mean', unit: '%' },
			{ col: 'c_climate', labelKey: 'sat.pm25.climate', aggregation: 'mean', unit: '%' },
			{ col: 'c_terrain', labelKey: 'sat.pm25.terrain', aggregation: 'mean', unit: '%' },
			{ col: 'c_vegetation', labelKey: 'sat.pm25.vegetation', aggregation: 'mean', unit: '%' },
		],
		primaryVariable: 'c_pm25_mean',
		colorScale: 'sequential',
		aggregation: 'mean',
		petalVars: [
			{ col: 'c_fire', labelKey: 'sat.pm25.fire', aggregation: 'mean' },
			{ col: 'c_climate', labelKey: 'sat.pm25.climate', aggregation: 'mean' },
			{ col: 'c_terrain', labelKey: 'sat.pm25.terrain', aggregation: 'mean' },
			{ col: 'c_vegetation', labelKey: 'sat.pm25.vegetation', aggregation: 'mean' },
		],
		titleKey: 'sat.pm25.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2013–2022', baseline: '2001–2010', source: 'ACAG V6' },
		legendLowKey: 'legend.pm25.low',
		legendHighKey: 'legend.pm25.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available'},
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
		colorScale: 'sequential',
		aggregation: 'mean',
		titleKey: 'sat.deforest.title',
		perDepartment: true,
		temporal: true,
		temporalPeriods: { current: '2015–2024', baseline: '2001–2010', source: 'Hansen GFC v1.12' },
		legendLowKey: 'legend.deforest.low',
		legendHighKey: 'legend.deforest.high',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available'},
	},
	// ── EUDR deforestation risk (H3 res-7, 10 provinces) ──
	eudr: {
		id: 'eudr',
		parquet: 'eudr_deforestation',
		variables: [
			{ col: 'risk_score', labelKey: 'eudr.riskScore', aggregation: 'mean' },
			{ col: 'loss_post_2020_pct', labelKey: 'eudr.lossPost2020', aggregation: 'mean', unit: '%' },
			{ col: 'fire_post_2020_pct', labelKey: 'eudr.firePost2020', aggregation: 'mean', unit: '%' },
			{ col: 'forest_cover_2020', labelKey: 'eudr.forest2020', aggregation: 'mean', unit: '%' },
			{ col: 'forest_cover_current', labelKey: 'eudr.forestCurrent', aggregation: 'mean', unit: '%' },
		],
		primaryVariable: 'risk_score',
		colorScale: 'flood',
		aggregation: 'mean',
		petalVars: [
			{ col: 'risk_score', labelKey: 'eudr.riskScore', aggregation: 'mean' },
			{ col: 'loss_post_2020_pct', labelKey: 'eudr.lossPost2020', aggregation: 'mean' },
			{ col: 'fire_post_2020_pct', labelKey: 'eudr.firePost2020', aggregation: 'mean' },
			{ col: 'forest_cover_2020', labelKey: 'eudr.forest2020', aggregation: 'mean' },
		],
		titleKey: 'trade.eudr.analysis_title',
		perDepartment: false,
		legendLowKey: 'legend.eudr.low',
		legendHighKey: 'legend.eudr.high',
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

export const ANALYSIS_REGISTRY: AnalysisConfig[] = [
	// ── Ambiente y riesgo ──
	{
		id: 'flood_risk',
		lensId: 'ambiente',
		titleKey: 'analysis.floodRisk.title',
		descKey: 'analysis.floodRisk.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available'},
		comparable: true,
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'carbon_stock',
		lensId: 'ambiente',
		titleKey: 'sat.carbon.title',
		descKey: 'sat.carbon.desc',
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available', concepcion_py: 'available', san_pedro_py: 'available', cordillera_py: 'available', guaira_py: 'available', caaguazu_py: 'available', caazapa_py: 'available', misiones_py: 'available', paraguari_py: 'available', central_py: 'available', neembucu_py: 'available', amambay_py: 'available', canindeyu_py: 'available', presidente_hayes_py: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
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
		coverage: { alto_parana_py: 'available', itapua_py: 'available', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'},
	},
	{
		id: 'sociodemographic',
		lensId: 'poblacion',
		titleKey: 'analysis.socio.title',
		descKey: 'analysis.socio.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'service_deprivation',
		lensId: 'poblacion',
		titleKey: 'sat.deprivation.title',
		descKey: 'sat.deprivation.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
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
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
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
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
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
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
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
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'available', formosa: 'available', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
		rigorBadge: 'census',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'eudr',
		lensId: 'economia',
		titleKey: 'trade.eudr.analysis_title',
		descKey: 'trade.eudr.analysis_desc',
		// EUDR is a global dataset over 10 NOA+NEA provinces, NOT a per-territory
		// comparable analysis. Not comparable; loads its own global parquet.
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'available', chaco: 'unavailable', formosa: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
	{
		id: 'powerline_density',
		lensId: 'economia',
		titleKey: 'emsa.title',
		descKey: 'emsa.desc',
		coverage: { alto_parana_py: 'unavailable', itapua_py: 'unavailable', corrientes: 'unavailable', chaco: 'unavailable', formosa: 'unavailable', parana_br: 'unavailable', santa_catarina_br: 'unavailable', rio_grande_sul_br: 'unavailable'},
		rigorBadge: 'physical',
		status: 'available',
		spatialUnit: 'hexagon',
	},
];

// ── Territorial Scores definitions ──────────────────────────────────────────
export const TERRITORIAL_SCORE_COLS = [
	'paving_index', 'urban_consolidation', 'service_access',
	'commercial_vitality', 'road_connectivity', 'building_mix',
	'urbanization', 'water_exposure',
] as const;

export const TERRITORIAL_SCORE_LABELS: Record<string, { es: string; en: string }> = {
	paving_index:         { es: 'Pavimentación',    en: 'Paving' },
	urban_consolidation:  { es: 'Consolidación',    en: 'Consolidation' },
	service_access:       { es: 'Servicios',        en: 'Services' },
	commercial_vitality:  { es: 'Comercio',         en: 'Commerce' },
	road_connectivity:    { es: 'Conectividad',     en: 'Connectivity' },
	building_mix:         { es: 'Mix tipológico',   en: 'Building Mix' },
	urbanization:         { es: 'Urbanización',     en: 'Urbanisation' },
	water_exposure:       { es: 'Exp. hídrica',     en: 'Water Exp.' },
};

export const TERRITORIAL_SCORE_DESCS: Record<string, { es: string; en: string }> = {
	paving_index:         { es: '% de calles pavimentadas en radio de 3 km', en: '% paved roads within 3 km' },
	urban_consolidation:  { es: 'Densidad edilicia + residencial + pavimentación + red vial', en: 'Building density + residential + paving + road network' },
	service_access:       { es: 'Salud, educación, farmacia y combustible en radio de 5 km', en: 'Health, education, pharmacy and fuel within 5 km' },
	commercial_vitality:  { es: 'Diversidad y densidad de comercios y servicios en 3 km', en: 'Diversity and density of commerce and services within 3 km' },
	road_connectivity:    { es: 'Jerarquía vial + densidad de segmentos + puentes', en: 'Road hierarchy + segment density + bridges' },
	building_mix:         { es: 'Diversidad de tipos: residencial, comercial, educativo, salud', en: 'Building type diversity: residential, commercial, education, health' },
	urbanization:         { es: 'Grado de desarrollo urbano (densidad edilicia + uso del suelo)', en: 'Degree of urban development (building density + land use)' },
	water_exposure:       { es: 'Densidad de ríos y arroyos en radio de 3 km', en: 'River and stream density within 3 km' },
};

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
			{ col: 'landslide_score', label: { es: 'Deslizamiento', en: 'Landslide' }, desc: { es: 'Susceptibilidad a deslizamientos de suelo', en: 'Soil landslide susceptibility' }, invert: false },
			{ col: 'rusle_mean', label: { es: 'Erosión', en: 'Erosion' }, desc: { es: 'Potencial de erosión hídrica (RUSLE)', en: 'Water erosion potential (RUSLE)' }, invert: false },
			{ col: 'slope_mean', label: { es: 'Pendiente', en: 'Slope' }, desc: { es: 'Pendiente media del terreno (%)', en: 'Mean terrain slope (%)' }, invert: false },
			{ col: 'deforest_pressure_score', label: { es: 'Presión deforest.', en: 'Deforestation' }, desc: { es: 'Presión de deforestación en la zona', en: 'Deforestation pressure in the area' }, invert: false },
		],
		howToRead: { es: 'Las parcelas se colorean según el score de riesgo de deslizamiento. Colores cálidos = mayor riesgo. El pétalo muestra 5 riesgos simultáneos: mayor extensión = mayor exposición.', en: 'Parcels are coloured by landslide risk score. Warmer colours = higher risk. The petal shows 5 simultaneous risks.' },
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
		dataDate: 'abril 2026',
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
	emsa_powerlines: { dataDate: 'EMSA abril 2024', processedDate: '27/03/2026', sourceKey: 'data.source.emsa' },
	sat_sociodemographic: { dataDate: 'Censo Nacional 2022 (INDEC)', processedDate: '29/03/2026', sourceKey: 'data.source.censo' },
	sat_economic_activity: { dataDate: 'Censo 2022 + VIIRS 2022-2024 + GBA 2025', processedDate: '29/03/2026', sourceKey: 'data.source.satellite' },
	sat_accessibility: { dataDate: 'Nelson 2019 / Oxford MAP 2019 / OSM', processedDate: '30/05/2026', sourceKey: 'data.source.satellite' },
	sat_carbon_stock: { dataDate: 'ESA CCI Biomass / GEDI / SoilGrids / MODIS NPP', processedDate: '20/04/2026', sourceKey: 'data.source.satellite' },
	eudr_deforestation: { dataDate: 'Hansen GFC v1.12 + MODIS MCD64A1 (cutoff 31/12/2020)', processedDate: '27/03/2026', sourceKey: 'data.source.satellite' },
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
	return `${R2_EUDR_BASE}/${name}.parquet?v=24`;
}

export function getEudrProvinceParquetUrl(province: string): string {
	return `${R2_EUDR_BASE}/by_province/eudr_${province}.parquet?v=20`;
}

export const EUDR_RISK_COLORS = {
	low: '#22c55e',
	medium: '#f59e0b',
	high: '#ef4444',
	critical: '#991b1b',
	unknown: '#6b7280',
};
