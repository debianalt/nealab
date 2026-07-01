<script lang="ts">
	import { onMount } from 'svelte';
	import maplibregl from 'maplibre-gl';
	import 'maplibre-gl/dist/maplibre-gl.css';
	import { Protocol } from 'pmtiles';
	import { getTilesUrl, BASEMAP, MAP_INIT, MAP_PROVINCE, TERRAIN_CONFIG } from '$lib/config';
	import { MapStore } from '$lib/stores/map.svelte';
	import { i18n } from '$lib/stores/i18n.svelte';
	import { formatDept } from '$lib/utils/format';
	import misionesBoundary from '$lib/data/misiones_boundary.json';
	import { ensureArBoundaries, getArFeatures } from '$lib/utils/deptBoundaries';
	import { pointInPolygon } from '$lib/utils/geometry';
	import misionesMask from '$lib/data/misiones_mask.json';
	import itapuaBoundary from '$lib/data/itapua_boundary.json';
	import itapuaMask from '$lib/data/itapua_mask.json';
	import corrientesBoundary from '$lib/data/corrientes_boundary.json';
	import corrientesMask from '$lib/data/corrientes_mask.json';
	import altoParanaBoundary from '$lib/data/alto_parana_boundary.json';
	import altoParanaMask from '$lib/data/alto_parana_mask.json';
	import chacoBoundary from '$lib/data/chaco_boundary.json';
	import formosaBoundary from '$lib/data/formosa_boundary.json';
	import paranaBrBoundary from '$lib/data/parana_br_boundary.json';
	import scBrBoundary from '$lib/data/santa_catarina_br_boundary.json';
	import rsBrBoundary from '$lib/data/rio_grande_sul_br_boundary.json';
	// New PY department boundaries (always-visible pink borders)
	import concepcionPyBoundary from '$lib/data/concepcion_py_boundary.json';
	import sanPedroPyBoundary from '$lib/data/san_pedro_py_boundary.json';
	import cordilleraPyBoundary from '$lib/data/cordillera_py_boundary.json';
	import guairaPyBoundary from '$lib/data/guaira_py_boundary.json';
	import caaguazuPyBoundary from '$lib/data/caaguazu_py_boundary.json';
	import caazapaPyBoundary from '$lib/data/caazapa_py_boundary.json';
	import misionesPyBoundary from '$lib/data/misiones_py_boundary.json';
	import paraguariPyBoundary from '$lib/data/paraguari_py_boundary.json';
	import centralPyBoundary from '$lib/data/central_py_boundary.json';
	import neembucuPyBoundary from '$lib/data/neembucu_py_boundary.json';
	import amambayPyBoundary from '$lib/data/amambay_py_boundary.json';
	import canindeyuPyBoundary from '$lib/data/canindeyu_py_boundary.json';
	import presidenteHayesPyBoundary from '$lib/data/presidente_hayes_py_boundary.json';
	import boqueronPyBoundary from '$lib/data/boqueron_py_boundary.json';
	import altoParaguayPyBoundary from '$lib/data/alto_paraguay_py_boundary.json';
	import { isInsideMisiones } from '$lib/utils/misiones-pip';
	import { isInsideItapua } from '$lib/utils/itapua-pip';
	import { isInsideCorrientes } from '$lib/utils/corrientes-pip';
	import { isInsideAltoParana } from '$lib/utils/alto_parana-pip';

	let { mapStore }: { mapStore: MapStore } = $props();

	let container: HTMLDivElement;
	let hexLayerTitle = '';
	let hexLayerIsCategorical = false;
	let hexLayerUnit = ''; // physical unit of the primary variable (tC/ha, min, %…); '' = no suffix
	let map: maplibregl.Map;
	let firstSymbolId: string | undefined; // basemap's first label layer — hex fills sit below it
	let lassoActive = false;
	let catastroActive = false;
	let activeTerritoryId = 'misiones';
	let regionalModeActive = false;
	// Forestry plantations overlay (DNDFI). Visibility is driven from the right panel
	// (mapStore.plantationsVisible), only when forestry_aptitude is active with a dept
	// selected. Per-territory tiles, gated by minzoom 9 so they load per-department.
	const PLANTATION_TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa'];

	onMount(() => {
		const protocol = new Protocol();
		maplibregl.addProtocol('pmtiles', protocol.tile);

		map = new maplibregl.Map({
			container,
			style: BASEMAP,
			center: MAP_INIT.center,
			zoom: MAP_INIT.zoom,
			pitch: MAP_INIT.pitch,
			bearing: MAP_INIT.bearing,
			minZoom: MAP_INIT.minZoom,
			maxZoom: MAP_INIT.maxZoom,
			antialias: true,
			attributionControl: false
		});

		map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'bottom-right');
		map.addControl(new maplibregl.AttributionControl({ compact: false }), 'bottom-right');

		map.on('error', (e) => console.error('MAP ERROR:', e.error?.message || e));

		map.on('load', () => {
			// Signal readiness to +page (drives the regional/compare viewport-load effect,
			// which otherwise can't re-run once the map finishes initializing — getMap() is
			// not reactive). Bubbles up to the mapContainer that +page listens on.
			map.getContainer().dispatchEvent(new CustomEvent('map-ready', { bubbles: true }));
			// Terrain DEM source (AWS Terrain Tiles, Terrarium encoding)
			map.addSource('terrain-dem', {
				type: 'raster-dem',
				tiles: [getTilesUrl('terrain')],
				encoding: 'terrarium',
				tileSize: 256
			});

			// Activate 3D terrain
			map.setTerrain({ source: 'terrain-dem', exaggeration: TERRAIN_CONFIG.exaggeration });

			// Radios source (PMTiles) — province boundary context
			map.addSource('radios', { type: 'vector', url: getTilesUrl('radios') });

			// Mask: fog outside Misiones (light overlay on dark basemap)
			// Visibility starts 'none' to match itapua/corrientes/AP masks — otherwise
			// the cold-open (regional view) briefly darkens everything outside Misiones
			// before setRegionalMapMode(true) hides it, producing a flash that read as
			// "NEA-AR looks selected". applyTerritoryVisibility() flips it on only when
			// Misiones becomes the active territory outside regional mode.
			map.addSource('mask', { type: 'geojson', data: misionesMask as any });
			map.addLayer({
				id: 'mask-fill',
				type: 'fill',
				source: 'mask',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#1a1a2e', 'fill-opacity': 0.75 }
			});

			// Mask: fog outside Itapúa — same style, hidden until territory switches
			map.addSource('itapua-mask', { type: 'geojson', data: itapuaMask as any });
			map.addLayer({
				id: 'itapua-mask-fill',
				type: 'fill',
				source: 'itapua-mask',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#1a1a2e', 'fill-opacity': 0.75 }
			});

			// Hillshade (subtle, complements dark basemap)
			map.addLayer({
				id: 'hillshade',
				type: 'hillshade',
				source: 'terrain-dem',
				paint: {
					'hillshade-shadow-color': TERRAIN_CONFIG.hillshade.shadowColor,
					'hillshade-highlight-color': TERRAIN_CONFIG.hillshade.highlightColor,
					'hillshade-illumination-direction': TERRAIN_CONFIG.hillshade.illuminationDirection,
					'hillshade-exaggeration': TERRAIN_CONFIG.hillshade.exaggeration
				}
			});


				// Province fill
			map.addLayer({
				id: 'province-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.06 }
			});

			// Province/radio borders
			map.addLayer({
				id: 'province-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				layout: { visibility: 'none' },
				paint: {
					'line-color': '#d4d4d4',
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						6, 1.2,
						10, 0.6,
						14, 0.3
					],
					'line-opacity': [
						'interpolate', ['linear'], ['zoom'],
						6, 0.3,
						10, 0.25,
						14, 0.15
					]
				}
			});

			// Province border: neon green outline
			map.addSource('province-boundary', { type: 'geojson', data: misionesBoundary as any });
			map.addLayer({
				id: 'province-border',
				type: 'line',
				source: 'province-boundary',
				paint: {
					'line-color': '#f472b6',
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						6, 1.2,
						9, 1.0,
						12, 0.8,
						16, 0.5
					],
					'line-opacity': [
						'interpolate', ['linear'], ['zoom'],
						6, 0.7,
						12, 0.5,
						16, 0.3
					]
				},
				layout: { 'line-join': 'round', 'line-cap': 'round' }
			});

			// Itapúa territory border — always visible, indicates available coverage
			map.addSource('itapua-boundary', { type: 'geojson', data: itapuaBoundary as any });
			map.addLayer({
				id: 'itapua-border',
				type: 'line',
				source: 'itapua-boundary',
				paint: {
					'line-color': '#f472b6',
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						6, 1.2,
						9, 1.0,
						12, 0.8,
						16, 0.5
					],
					'line-opacity': [
						'interpolate', ['linear'], ['zoom'],
						6, 0.7,
						12, 0.5,
						16, 0.3
					]
				},
				layout: { 'line-join': 'round', 'line-cap': 'round' }
			});

			// Corrientes mask + border (hidden until territory switches)
			map.addSource('corrientes-mask', { type: 'geojson', data: corrientesMask as any });
			// Insert before province-fill so census radios render on top (same pattern as Misiones mask)
			map.addLayer({
				id: 'corrientes-mask-fill',
				type: 'fill',
				source: 'corrientes-mask',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#1a1a2e', 'fill-opacity': 0.75 }
			}, 'province-fill');
			map.addSource('corrientes-boundary', { type: 'geojson', data: corrientesBoundary as any });
			map.addLayer({
				id: 'corrientes-border',
				type: 'line',
				source: 'corrientes-boundary',
				layout: { 'line-join': 'round', 'line-cap': 'round' },
				paint: {
					'line-color': '#f472b6',
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						6, 1.2,
						9, 1.0,
						12, 0.8,
						16, 0.5
					],
					'line-opacity': [
						'interpolate', ['linear'], ['zoom'],
						6, 0.7,
						12, 0.5,
						16, 0.3
					]
				}
			});

			// Alto Paraná (PY) mask + border — secondary territory, mirrors Corrientes
			map.addSource('alto_parana-mask', { type: 'geojson', data: altoParanaMask as any });
			map.addLayer({
				id: 'alto_parana-mask-fill',
				type: 'fill',
				source: 'alto_parana-mask',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#1a1a2e', 'fill-opacity': 0.75 }
			}, 'province-fill');
			map.addSource('alto_parana-boundary', { type: 'geojson', data: altoParanaBoundary as any });
			map.addLayer({
				id: 'alto_parana-border',
				type: 'line',
				source: 'alto_parana-boundary',
				layout: { 'line-join': 'round', 'line-cap': 'round' },
				paint: {
					'line-color': '#f472b6',
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						6, 1.2,
						9, 1.0,
						12, 0.8,
						16, 0.5
					],
					'line-opacity': [
						'interpolate', ['linear'], ['zoom'],
						6, 0.7,
						12, 0.5,
						16, 0.3
					]
				}
			});

			// v1.1 new territories — always-visible borders (same pattern as Mis/Ita/Cor/AP)
			const v11Borders: Array<[string, any]> = [
				['chaco', chacoBoundary],
				['formosa', formosaBoundary],
				['parana_br', paranaBrBoundary],
				['santa_catarina_br', scBrBoundary],
				['rio_grande_sul_br', rsBrBoundary],
			];
			for (const [id, data] of v11Borders) {
				map.addSource(`${id}-boundary`, { type: 'geojson', data });
				map.addLayer({
					id: `${id}-border`,
					type: 'line',
					source: `${id}-boundary`,
					layout: { 'line-join': 'round', 'line-cap': 'round' },
					paint: {
						'line-color': '#f472b6',
						'line-width': [
							'interpolate', ['linear'], ['zoom'],
							6, 1.2, 9, 1.0, 12, 0.8, 16, 0.5
						],
						'line-opacity': [
							'interpolate', ['linear'], ['zoom'],
							6, 0.7, 12, 0.5, 16, 0.3
						]
					}
				});
			}

			// New PY departments — always-visible pink borders (same pattern as v11Borders)
			const pyDeptBorders: Array<[string, any]> = [
				['concepcion_py',       concepcionPyBoundary],
				['san_pedro_py',        sanPedroPyBoundary],
				['cordillera_py',       cordilleraPyBoundary],
				['guaira_py',           guairaPyBoundary],
				['caaguazu_py',         caaguazuPyBoundary],
				['caazapa_py',          caazapaPyBoundary],
				['misiones_py',         misionesPyBoundary],
				['paraguari_py',        paraguariPyBoundary],
				['central_py',          centralPyBoundary],
				['neembucu_py',         neembucuPyBoundary],
				['amambay_py',          amambayPyBoundary],
				['canindeyu_py',        canindeyuPyBoundary],
				['presidente_hayes_py', presidenteHayesPyBoundary],
			['boqueron_py',         boqueronPyBoundary],
			['alto_paraguay_py',    altoParaguayPyBoundary],
			];
			for (const [id, data] of pyDeptBorders) {
				map.addSource(`${id}-boundary`, { type: 'geojson', data });
				map.addLayer({
					id: `${id}-border`,
					type: 'line',
					source: `${id}-boundary`,
					layout: { 'line-join': 'round', 'line-cap': 'round' },
					paint: {
						'line-color': '#f472b6',
						'line-width': [
							'interpolate', ['linear'], ['zoom'],
							6, 1.2, 9, 1.0, 12, 0.8, 16, 0.5
						],
						'line-opacity': [
							'interpolate', ['linear'], ['zoom'],
							6, 0.7, 12, 0.5, 16, 0.3
						]
					}
				});
			}

			// Buildings source (PMTiles)
			map.addSource('buildings', { type: 'vector', url: getTilesUrl('buildings') });

			// 3D fill-extrusion layer
			map.addLayer({
				id: 'buildings-3d',
				type: 'fill-extrusion',
				source: 'buildings',
				'source-layer': 'buildings',
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Forestry plantations overlay (DNDFI Inventario Nacional 2026) — one vector
			// source per AR territory, hidden until forestry_aptitude is active AND the
			// user toggles it on (updatePlantationsVisibility). It is the same inventory
			// the SDM uses as presence, so the score and the overlay are one object.
			for (const t of ['misiones', 'corrientes', 'chaco', 'formosa'] as const) {
				map.addSource(`${t}-plantations`, { type: 'vector', url: getTilesUrl(`${t}_plantations` as any) });
				// No minzoom: the overlay is filtered to the selected department (setFilter
				// on `depto` in updatePlantationsVisibility), so only that dept's polygons
				// render — light at any zoom, and loaded per-department, not by zoom level.
				map.addLayer({
					id: `${t}-plantations-fill`,
					type: 'fill',
					source: `${t}-plantations`,
					'source-layer': 'plantations',
					layout: { visibility: 'none' },
					paint: {
						// Differentiate plantation type by species group (DNDFI grupo_espe).
						'fill-color': ['match', ['get', 'grupo_espe'],
							'Pinos', '#22c55e',
							'Eucaliptos', '#38bdf8',
							'Nativas', '#f59e0b',
							'Sauces y Álamos', '#a78bfa',
							/* otras / sin clasificar */ '#9ca3af'],
						'fill-opacity': 0.5
					}
				});
				map.addLayer({
					id: `${t}-plantations-line`,
					type: 'line',
					source: `${t}-plantations`,
					'source-layer': 'plantations',
					layout: { visibility: 'none' },
					paint: { 'line-color': '#b45309', 'line-width': 0.8, 'line-opacity': 0.9 }
				});
			}

			// Itapúa buildings (pre-created, hidden until territory switch)
			map.addSource('itapua-buildings', { type: 'vector', url: getTilesUrl('itapua_buildings') });
			map.addLayer({
				id: 'itapua-buildings-3d',
				type: 'fill-extrusion',
				source: 'itapua-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Itapúa district polygons (31 GAUL distritos, hidden until territory switch)
			map.addSource('itapua-districts', { type: 'vector', url: getTilesUrl('itapua_districts') });
			const emptyDistrictFilter: any = ['==', ['get', 'district'], ''];
			map.addLayer({
				id: 'itapua-district-fill',
				type: 'fill',
				source: 'itapua-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.06 }
			});
			map.addLayer({
				id: 'itapua-district-line',
				type: 'line',
				source: 'itapua-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				// Hide at low zoom: source drift creates double outline at continent view.
				// Above zoom 8 the user is intentionally looking inside Itapúa.
				minzoom: 8,
				paint: {
					'line-color': '#f472b6',
					'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 13, 0.8],
					'line-opacity': 0.35
				}
			});
			map.addLayer({
				id: 'itapua-district-selected-fill',
				type: 'fill',
				source: 'itapua-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.45 },
				filter: emptyDistrictFilter
			});
			map.addLayer({
				id: 'itapua-district-selected-line',
				type: 'line',
				source: 'itapua-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 1 },
				filter: emptyDistrictFilter
			});

			// Alto Paraná buildings (Overture + DGEEC 2022; hidden until territory switch)
			map.addSource('alto_parana-buildings', { type: 'vector', url: getTilesUrl('alto_parana_buildings') });
			map.addLayer({
				id: 'alto_parana-buildings-3d',
				type: 'fill-extrusion',
				source: 'alto_parana-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Alto Paraná district polygons (22 INE 2022 distritos, hidden until territory switch)
			map.addSource('alto_parana-districts', { type: 'vector', url: getTilesUrl('alto_parana_districts') });
			map.addLayer({
				id: 'alto_parana-district-fill',
				type: 'fill',
				source: 'alto_parana-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.06 }
			});
			map.addLayer({
				id: 'alto_parana-district-line',
				type: 'line',
				source: 'alto_parana-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				// See itapua-district-line: tile/boundary source drift.
				minzoom: 8,
				paint: {
					'line-color': '#f472b6',
					'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 13, 0.8],
					'line-opacity': 0.35
				}
			});
			map.addLayer({
				id: 'alto_parana-district-selected-fill',
				type: 'fill',
				source: 'alto_parana-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.45 },
				filter: emptyDistrictFilter
			});
			map.addLayer({
				id: 'alto_parana-district-selected-line',
				type: 'line',
				source: 'alto_parana-districts',
				'source-layer': 'districts',
				layout: { visibility: 'none' },
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 1 },
				filter: emptyDistrictFilter
			});

			// ── Paraguay departments — 15 new territories (GAUL districts + Overture buildings) ──
			// Pattern mirrors Itapúa/Alto Paraná. Buildings hidden until territory switch.
			// District tiles available immediately; satellite data pending GEE exports.
			const PY_TERR_LIST = [
				['concepcion_py', 'concepcion_py_buildings', 'concepcion_py_districts'],
				['san_pedro_py', 'san_pedro_py_buildings', 'san_pedro_py_districts'],
				['cordillera_py', 'cordillera_py_buildings', 'cordillera_py_districts'],
				['guaira_py', 'guaira_py_buildings', 'guaira_py_districts'],
				['caaguazu_py', 'caaguazu_py_buildings', 'caaguazu_py_districts'],
				['caazapa_py', 'caazapa_py_buildings', 'caazapa_py_districts'],
				['misiones_py', 'misiones_py_buildings', 'misiones_py_districts'],
				['paraguari_py', 'paraguari_py_buildings', 'paraguari_py_districts'],
				['central_py', 'central_py_buildings', 'central_py_districts'],
				['neembucu_py', 'neembucu_py_buildings', 'neembucu_py_districts'],
				['amambay_py', 'amambay_py_buildings', 'amambay_py_districts'],
				['canindeyu_py', 'canindeyu_py_buildings', 'canindeyu_py_districts'],
				['presidente_hayes_py', 'presidente_hayes_py_buildings', 'presidente_hayes_py_districts'],
				['boqueron_py', 'boqueron_py_buildings', 'boqueron_py_districts'],
				['alto_paraguay_py', 'alto_paraguay_py_buildings', 'alto_paraguay_py_districts'],
			] as const;

			for (const [tid, bldgKey, distKey] of PY_TERR_LIST) {
				// Buildings use the stripped ID (without _py) — matches TERRITORY_BUILDINGS_LAYER keys
				const srcId = tid.replace(/_py$/, '');
				// Districts use the full territory ID — matches PY_DISTRICT_LAYERS + setDeptPickerVisible
				const distId = tid;
				// Buildings source + 3D layer
				map.addSource(`${srcId}-buildings`, { type: 'vector', url: getTilesUrl(bldgKey as any) });
				map.addLayer({
					id: `${srcId}-buildings-3d`,
					type: 'fill-extrusion',
					source: `${srcId}-buildings`,
					'source-layer': 'buildings',
					layout: { visibility: 'none' },
					paint: {
						'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
						'fill-extrusion-base': 0,
						'fill-extrusion-color': mapStore.getColorExpr() as any,
						'fill-extrusion-opacity': 0.92
					}
				});
				// District source + 4 layers (fill, line, selected-fill, selected-line)
				map.addSource(`${distId}-districts`, { type: 'vector', url: getTilesUrl(distKey as any) });
				map.addLayer({
					id: `${distId}-district-fill`,
					type: 'fill', source: `${distId}-districts`, 'source-layer': 'districts',
					layout: { visibility: 'none' },
					paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.06 }
				});
				map.addLayer({
					id: `${distId}-district-line`,
					type: 'line', source: `${distId}-districts`, 'source-layer': 'districts',
					layout: { visibility: 'none' }, minzoom: 8,
					paint: { 'line-color': '#f472b6', 'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.5, 13, 0.8], 'line-opacity': 0.35 }
				});
				map.addLayer({
					id: `${distId}-district-selected-fill`,
					type: 'fill', source: `${distId}-districts`, 'source-layer': 'districts',
					layout: { visibility: 'none' },
					paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.45 },
					filter: emptyDistrictFilter
				});
				map.addLayer({
					id: `${distId}-district-selected-line`,
					type: 'line', source: `${distId}-districts`, 'source-layer': 'districts',
					layout: { visibility: 'none' },
					paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 1 },
					filter: emptyDistrictFilter
				});
			}

			// Corrientes buildings (pre-created, hidden until territory switch)
			map.addSource('corrientes-buildings', { type: 'vector', url: getTilesUrl('corrientes_buildings') });
			map.addLayer({
				id: 'corrientes-buildings-3d',
				type: 'fill-extrusion',
				source: 'corrientes-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Chaco buildings (AR census, mirrors Corrientes; hidden until territory switch)
			map.addSource('chaco-buildings', { type: 'vector', url: getTilesUrl('chaco_buildings') });
			map.addLayer({
				id: 'chaco-buildings-3d',
				type: 'fill-extrusion',
				source: 'chaco-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Formosa buildings (AR census, mirrors Corrientes; hidden until territory switch)
			map.addSource('formosa-buildings', { type: 'vector', url: getTilesUrl('formosa_buildings') });
			map.addLayer({
				id: 'formosa-buildings-3d',
				type: 'fill-extrusion',
				source: 'formosa-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Paraná (BR) buildings — footprints only, no census (hidden until territory switch)
			map.addSource('parana_br-buildings', { type: 'vector', url: getTilesUrl('parana_br_buildings') });
			map.addLayer({
				id: 'parana_br-buildings-3d',
				type: 'fill-extrusion',
				source: 'parana_br-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Santa Catarina (BR) buildings — footprints only, no census (hidden until territory switch)
			map.addSource('santa_catarina_br-buildings', { type: 'vector', url: getTilesUrl('santa_catarina_br_buildings') });
			map.addLayer({
				id: 'santa_catarina_br-buildings-3d',
				type: 'fill-extrusion',
				source: 'santa_catarina_br-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Rio Grande do Sul (BR) buildings — footprints only, no census (hidden until territory switch)
			map.addSource('rio_grande_sul_br-buildings', { type: 'vector', url: getTilesUrl('rio_grande_sul_br_buildings') });
			map.addLayer({
				id: 'rio_grande_sul_br-buildings-3d',
				type: 'fill-extrusion',
				source: 'rio_grande_sul_br-buildings',
				'source-layer': 'buildings',
				layout: { visibility: 'none' },
				paint: {
					'fill-extrusion-height': ['min', ['max', ['coalesce', ['get', 'best_height_m'], 5], 5], 300],
					'fill-extrusion-base': 0,
					'fill-extrusion-color': mapStore.getColorExpr() as any,
					'fill-extrusion-opacity': 0.92
				}
			});

			// Lighting (adjusted for terrain + buildings interaction)
			map.setLight({
				anchor: 'viewport',
				color: '#e0f0ff',
				intensity: 0.55,
				position: [1.5, 210, 35]
			});

			// Opportunity glow layers (pre-created, updated by setOpportunityGlow)
			const emptyFilter: any = ['==', ['get', 'redcode'], ''];
			map.addLayer({
				id: 'opportunity-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'fill-color': '#22c55e', 'fill-opacity': 0.25 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'opportunity-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'line-color': '#22c55e', 'line-width': 2, 'line-opacity': 0.7 },
				filter: emptyFilter
			});

			// Selected radio layers (pre-created, updated by highlightSingleOpportunity)
			map.addLayer({
				id: 'selected-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'fill-color': '#ffffff', 'fill-opacity': 0.45 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'selected-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'line-color': '#ffffff', 'line-width': 3, 'line-opacity': 1 },
				filter: emptyFilter
			});

			// Radio highlight layer (building outlines at high zoom)
			map.addLayer({
				id: 'radio-highlight',
				type: 'line',
				source: 'buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 4.5, 'line-opacity': 0.8 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'radio-highlight-corrientes',
				type: 'line',
				source: 'corrientes-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 4.5, 'line-opacity': 0.8 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'radio-highlight-chaco',
				type: 'line',
				source: 'chaco-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 4.5, 'line-opacity': 0.8 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'radio-highlight-formosa',
				type: 'line',
				source: 'formosa-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 4.5, 'line-opacity': 0.8 },
				filter: emptyFilter
			});
			// Brazil: highlight layers on building tiles (setor outline via redcode filter)
			for (const [tid, src] of [
				['parana_br', 'parana_br-buildings'],
				['santa_catarina_br', 'santa_catarina_br-buildings'],
				['rio_grande_sul_br', 'rio_grande_sul_br-buildings'],
			] as [string, string][]) {
				map.addLayer({
					id: `radio-highlight-${tid}`,
					type: 'line',
					source: src,
					'source-layer': 'buildings',
					paint: { 'line-color': '#60a5fa', 'line-width': 4.5, 'line-opacity': 0.8 },
					filter: emptyFilter,
				});
			}

			// ── Lasso draw layers ──────────────────────────────────────────
			map.addSource('lasso-draw', {
				type: 'geojson',
				data: { type: 'Feature', geometry: { type: 'Polygon', coordinates: [[]] }, properties: {} }
			});
			map.addLayer({
				id: 'lasso-draw-fill',
				type: 'fill',
				source: 'lasso-draw',
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.15 }
			});
			map.addLayer({
				id: 'lasso-draw-line',
				type: 'line',
				source: 'lasso-draw',
				paint: { 'line-color': '#60a5fa', 'line-width': 2, 'line-dasharray': [4, 2] }
			});

			// ── Zone highlight layers (radios + buildings) ─────────────
			map.addLayer({
				id: 'zone-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.45 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'zone-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'line-color': '#60a5fa', 'line-width': 2.5, 'line-opacity': 0.9 },
				filter: emptyFilter
			});
			// ── Radio census-panel brush highlight (chart brush → radios) ──
			map.addLayer({
				id: 'radio-brush-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'fill-color': '#fbbf24', 'fill-opacity': 0.40 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'radio-brush-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'line-color': '#fbbf24', 'line-width': 1.5, 'line-opacity': 0.9 },
				filter: emptyFilter
			});
			// Building outlines tinted by zone color (visible in 3D)
			map.addLayer({
				id: 'zone-buildings',
				type: 'line',
				source: 'buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 0.85 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'zone-buildings-corrientes',
				type: 'line',
				source: 'corrientes-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 0.85 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'zone-buildings-chaco',
				type: 'line',
				source: 'chaco-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 0.85 },
				filter: emptyFilter
			});
			map.addLayer({
				id: 'zone-buildings-formosa',
				type: 'line',
				source: 'formosa-buildings',
				'source-layer': 'buildings',
				paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 0.85 },
				filter: emptyFilter
			});
			// Brazil zone-buildings highlight layers
			for (const [tid, src] of [
				['parana_br', 'parana_br-buildings'],
				['santa_catarina_br', 'santa_catarina_br-buildings'],
				['rio_grande_sul_br', 'rio_grande_sul_br-buildings'],
			] as [string, string][]) {
				map.addLayer({
					id: `zone-buildings-${tid}`,
					type: 'line',
					source: src,
					'source-layer': 'buildings',
					paint: { 'line-color': '#60a5fa', 'line-width': 3, 'line-opacity': 0.85 },
					filter: emptyFilter,
				});
			}

			// ── Department bbox outlines — visible at all zoom levels ────────
			map.addSource('dept-highlights', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'dept-highlight-fill',
				type: 'fill',
				source: 'dept-highlights',
				paint: { 'fill-color': ['get', 'color'], 'fill-opacity': 0.07 }
			});
			map.addLayer({
				id: 'dept-highlight-line',
				type: 'line',
				source: 'dept-highlights',
				paint: {
					'line-color': ['get', 'color'],
					'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2.5, 8, 1.5, 12, 1],
					'line-opacity': 0
				}
			});

			// ── AR department picker (Misiones + Corrientes clickable polygons) ──
			// Phase 2: click a department polygon → load that dept's hexes for the
			// active analysis. Shown only in the department-selection state.
			map.addSource('ar-depts', {
				type: 'geojson',
				// Seeded empty; loadArDeptSource() fills it (lazy 465KB) when the AR dept
				// picker is first shown for an AR territory.
				data: { type: 'FeatureCollection', features: [] } as any
			});
			map.addLayer({
				id: 'ar-dept-fill',
				type: 'fill',
				source: 'ar-depts',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#60a5fa', 'fill-opacity': 0.05 }
			});
			map.addLayer({
				id: 'ar-dept-line',
				type: 'line',
				source: 'ar-depts',
				layout: { visibility: 'none' },
				// Same low-zoom overlap mitigation as itapua/AP — keeps the pink
				// province border clean at continent view.
				minzoom: 8,
				paint: {
					'line-color': '#93c5fd',
					'line-width': ['interpolate', ['linear'], ['zoom'], 8, 0.6, 13, 0.6],
					'line-opacity': 0.5
				}
			});
			map.on('mouseenter', 'ar-dept-fill', () => { if (!lassoActive) map.getCanvas().style.cursor = 'pointer'; });
			map.on('mouseleave', 'ar-dept-fill', () => { if (!lassoActive) map.getCanvas().style.cursor = ''; });
			map.on('click', 'ar-dept-fill', (e) => {
				if (lassoActive) return;
				const p = e.features![0].properties!;
				const rc = String(p.redcode ?? '');
				// codprov INDEC → territory id (matches AR_PROVINCE_PREFIX in deptBoundaries.ts)
				const territoryByCodprov: Record<string, string> = {
					'54': 'misiones', '18': 'corrientes', '22': 'chaco', '34': 'formosa',
				};
				const territory = territoryByCodprov[rc.slice(0, 2)] ?? '';
				if (!p.nombre || !territory) return;
				container.dispatchEvent(new CustomEvent('dept-map-select', {
					bubbles: true,
					detail: { name: p.nombre, territory }
				}));
			});

			// ── Selected department outline (real polygon, single-dept mode) ──
			map.addSource('dept-outline', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'dept-outline-line',
				type: 'line',
				source: 'dept-outline',
				paint: {
					'line-color': '#60a5fa',
					'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 1.8, 14, 1.2],
					'line-opacity': 0.85
				}
			});

			// ── Compared department outline (real polygon, compare mode) ──
			map.addSource('compare-dept-outline', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'compare-dept-outline-line',
				type: 'line',
				source: 'compare-dept-outline',
				paint: {
					'line-color': '#fbbf24',
					'line-width': ['interpolate', ['linear'], ['zoom'], 4, 2, 8, 1.8, 14, 1.2],
					'line-opacity': 0.85
				}
			});

			// ── Hexagon H3 layers (GeoJSON, loaded dynamically) ─────────
			map.addSource('hexagons', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});

			// Territory background: fills missing-parquet hexes so they show gray
			// instead of pure dark basemap, eliminating the "manchones" patch effect.
			map.addSource('territory-bg', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});

			// Insert the hexagon fills BELOW the basemap's label/symbol layers so street and
			// place names stay readable on top of the choropleth (orientation). null = no symbol
			// layer found → fall back to adding on top (previous behaviour).
			firstSymbolId = (map.getStyle().layers ?? []).find(l => l.type === 'symbol')?.id ?? undefined;

			map.addLayer({
				id: 'hex-fill',
				type: 'fill',
				source: 'hexagons',
				paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0 }
			}, firstSymbolId);

			map.addLayer({
				id: 'territory-bg-fill',
				type: 'fill',
				source: 'territory-bg',
				paint: { 'fill-color': 'rgb(55,65,81)', 'fill-opacity': 0.65 }
			}, 'hex-fill');
			map.addLayer({
				id: 'hex-line',
				type: 'line',
				source: 'hexagons',
				paint: { 'line-color': '#1e293b', 'line-width': 0.5, 'line-opacity': 0 }
			}, firstSymbolId);
			map.addLayer({
				id: 'hex-selected',
				type: 'line',
				source: 'hexagons',
				paint: { 'line-color': '#ffffff', 'line-width': 3, 'line-opacity': 0.9 },
				filter: ['==', ['get', 'h3index'], '']
			}, firstSymbolId);

			// Improve label contrast over bright hexagons: dark halo on the basemap's text.
			for (const l of (map.getStyle().layers ?? [])) {
				if (l.type === 'symbol' && (l.layout as any)?.['text-field']) {
					try {
						map.setPaintProperty(l.id, 'text-halo-color', 'rgba(8,10,14,0.9)');
						map.setPaintProperty(l.id, 'text-halo-width', 1.4);
					} catch { /* layer may not support it */ }
				}
			}

			// EUDR mode: NOA+NEA province outlines (pink). Hidden unless EUDR active.
			map.addSource('eudr-provinces-main', {
				type: 'geojson',
				data: '/data/eudr_provinces_boundary.json'
			});
			map.addLayer({
				id: 'eudr-provinces-line',
				type: 'line',
				source: 'eudr-provinces-main',
				layout: { visibility: 'none' },
				paint: { 'line-color': '#ec4899', 'line-width': 1.5, 'line-opacity': 0.85 }
			});

			// Admin-2 (depts/distritos/municipios) for the EUDR area — thin white,
			// drawn ABOVE the hexagon choropleth so users can orient inside it.
			map.addSource('eudr-admin2', {
				type: 'geojson',
				data: '/data/eudr_admin2_boundary.json'
			});
			map.addLayer({
				id: 'eudr-admin2-line',
				type: 'line',
				source: 'eudr-admin2',
				layout: { visibility: 'none' },
				paint: { 'line-color': '#ffffff', 'line-width': 0.6, 'line-opacity': 0.32 }
			});

			// Invisible click target on admin-2 polygons — lets the user pick a
			// statistically meaningful unit (dept/distrito/municipio) to load.
			map.addLayer({
				id: 'eudr-admin2-fill',
				type: 'fill',
				source: 'eudr-admin2',
				layout: { visibility: 'none' },
				paint: { 'fill-color': '#ffffff', 'fill-opacity': 0.0001 }
			});
			map.on('mouseenter', 'eudr-admin2-fill', () => { map.getCanvas().style.cursor = 'pointer'; });
			map.on('mouseleave', 'eudr-admin2-fill', () => { map.getCanvas().style.cursor = ''; });
			map.on('click', 'eudr-admin2-fill', (e) => {
				const feat = e.features?.[0];
				if (!feat) return;
				container.dispatchEvent(new CustomEvent('eudr-unit-select', {
					bubbles: true,
					detail: { name: feat.properties?.name, country: feat.properties?.country, geometry: feat.geometry }
				}));
			});

			// EUDR focus: NEA + cross-border (PY/BR) area of interest — yellow,
			// thicker, drawn above admin-2 and the pink Argentina context.
			map.addSource('eudr-focus', {
				type: 'geojson',
				data: '/data/eudr_focus_boundary.json'
			});
			map.addLayer({
				id: 'eudr-focus-line',
				type: 'line',
				source: 'eudr-focus',
				layout: { visibility: 'none' },
				paint: { 'line-color': '#facc15', 'line-width': 2.5, 'line-opacity': 0.95 }
			});

			// ── Compare territory hex choropleth (dept comparison mode) ─────
			map.addSource('compare-hexagons', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'compare-hex-fill',
				type: 'fill',
				source: 'compare-hexagons',
				paint: { 'fill-color': '#0f172a', 'fill-opacity': 0 }
			});
			map.addLayer({
				id: 'compare-hex-line',
				type: 'line',
				source: 'compare-hexagons',
				paint: { 'line-color': '#0f172a', 'line-width': 0.5, 'line-opacity': 0 }
			});
			map.addLayer({
				id: 'compare-hex-selected',
				type: 'line',
				source: 'compare-hexagons',
				paint: { 'line-color': '#f59e0b', 'line-width': 3, 'line-opacity': 0.9 },
				filter: ['==', ['get', 'h3index'], '']
			});

			// ── Regional mode hex choropleth (3rd territory slot) ────────────
			map.addSource('regional-hexagons', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'regional-hex-fill',
				type: 'fill',
				source: 'regional-hexagons',
				paint: { 'fill-color': '#3b82f6', 'fill-opacity': 0 }
			});
			map.addLayer({
				id: 'regional-hex-line',
				type: 'line',
				source: 'regional-hexagons',
				paint: { 'line-color': '#1e293b', 'line-width': 0.5, 'line-opacity': 0 }
			});

			// ── Hex zone highlight layers (GeoJSON, for lasso zones) ────────
			map.addSource('hex-zones', {
				type: 'geojson',
				data: { type: 'FeatureCollection', features: [] }
			});
			map.addLayer({
				id: 'hex-zone-fill',
				type: 'fill',
				source: 'hex-zones',
				paint: { 'fill-color': ['get', 'color'], 'fill-opacity': 0.35 }
			});
			map.addLayer({
				id: 'hex-zone-line',
				type: 'line',
				source: 'hex-zones',
				paint: { 'line-color': ['get', 'color'], 'line-width': 2, 'line-opacity': 0.8 }
			});

			setupInteractions();

			// Re-apply territory visibility in case territory was set before map loaded
			// (e.g., territory restored from URL state before onMount completed)
			applyTerritoryVisibility();
			hideBasemapAdminLines();
			if (regionalModeActive) setRegionalMapMode(true);
		});

		return () => {
			maplibregl.removeProtocol('pmtiles');
			map.remove();
		};
	});

	function setupInteractions() {
		const tooltip = document.createElement('div');
		tooltip.id = 'hover-tooltip';
		tooltip.style.cssText = `
			position: fixed; pointer-events: none; z-index: 20; display: none;
			background: rgba(8,10,20,0.92); backdrop-filter: blur(12px);
			border: 1px solid rgba(96,165,250,0.3); border-radius: 8px;
			padding: 10px 14px; font-size: 12px; line-height: 1.7; color: #cbd5e1;
			box-shadow: 0 4px 24px rgba(0,0,0,0.6); max-width: 260px;
		`;
		document.body.appendChild(tooltip);

		let leaveTimeout: ReturnType<typeof setTimeout> | null = null;

		map.on('mousemove', 'buildings-3d', (e) => {
			if (lassoActive) return; // keep crosshair, skip tooltip
			if (leaveTimeout) { clearTimeout(leaveTimeout); leaveTimeout = null; }
			map.getCanvas().style.cursor = 'pointer';
			const p = e.features![0].properties!;

			const pers = parseInt(p.est_personas) || 0;
			const h = p.best_height_m != null ? parseFloat(p.best_height_m).toFixed(1) : '?';
			const a = p.area_m2 != null ? Math.round(p.area_m2).toLocaleString() : '?';
			const redcode = p.redcode || null;
			const radioPop = parseInt(p.radio_personas) || 0;
			const radioDens = p.densidad_hab_km2 != null ? Math.round(p.densidad_hab_km2).toLocaleString() : '?';
			const radioViv = parseInt(p.radio_viviendas) || 0;
			const radioHog = parseInt(p.radio_hogares) || 0;
			const radioAreaKm2 = p.radio_area_km2 != null ? parseFloat(p.radio_area_km2).toFixed(1) : '?';

			let html = `<b style="color:#60a5fa">${i18n.t('tip.building')}</b> ${i18n.t('tip.height')} ${h} m | ${i18n.t('tip.area')} ${a} m\u00B2<br>` +
				`<b style="color:#60a5fa">${i18n.t('tip.estPersons')}</b> <span style="color:#60a5fa;font-weight:600">${pers}</span>`;
			if (redcode) {
				html += `<br><span style="color:#a3a3a3">\u2500\u2500\u2500</span><br>` +
					`<b style="color:#d4d4d4">${i18n.t('tip.radio')}</b> <span style="color:#d4d4d4">${redcode}</span><br>` +
					`<b style="color:#d4d4d4">${i18n.t('tip.pop')}</b> ${radioPop.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('tip.density')}</b> ${radioDens} hab/km\u00B2<br>` +
					`<b style="color:#d4d4d4">${i18n.t('label.dwellings')}:</b> ${radioViv.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.households')}:</b> ${radioHog.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.area')}:</b> ${radioAreaKm2} km\u00B2`;
			}
			tooltip.innerHTML = html;
			tooltip.style.display = 'block';
			tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
			tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
		});

		map.on('mouseleave', 'buildings-3d', () => {
			leaveTimeout = setTimeout(() => {
				if (!lassoActive) map.getCanvas().style.cursor = '';
				tooltip.style.display = 'none';
			}, 80);
		});

		// Corrientes buildings tooltip (same census data as Misiones)
		map.on('mousemove', 'corrientes-buildings-3d', (e) => {
			if (lassoActive) return;
			if (leaveTimeout) { clearTimeout(leaveTimeout); leaveTimeout = null; }
			map.getCanvas().style.cursor = 'pointer';
			const p = e.features![0].properties!;

			const pers = parseInt(p.est_personas) || 0;
			const h = p.best_height_m != null ? parseFloat(p.best_height_m).toFixed(1) : '?';
			const a = p.area_m2 != null ? Math.round(p.area_m2).toLocaleString() : '?';
			const redcode = p.redcode || null;
			const radioPop = parseInt(p.radio_personas) || 0;
			const radioDens = p.densidad_hab_km2 != null ? Math.round(p.densidad_hab_km2).toLocaleString() : '?';
			const radioViv = parseInt(p.radio_viviendas) || 0;
			const radioHog = parseInt(p.radio_hogares) || 0;
			const radioAreaKm2 = p.radio_area_km2 != null ? parseFloat(p.radio_area_km2).toFixed(1) : '?';

			let html = `<b style="color:#60a5fa">${i18n.t('tip.building')}</b> ${i18n.t('tip.height')} ${h} m | ${i18n.t('tip.area')} ${a} m²<br>` +
				`<b style="color:#60a5fa">${i18n.t('tip.estPersons')}</b> <span style="color:#60a5fa;font-weight:600">${pers}</span>`;
			if (redcode) {
				html += `<br><span style="color:#a3a3a3">───</span><br>` +
					`<b style="color:#d4d4d4">${i18n.t('tip.radio')}</b> <span style="color:#d4d4d4">${redcode}</span><br>` +
					`<b style="color:#d4d4d4">${i18n.t('tip.pop')}</b> ${radioPop.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('tip.density')}</b> ${radioDens} hab/km²<br>` +
					`<b style="color:#d4d4d4">${i18n.t('label.dwellings')}:</b> ${radioViv.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.households')}:</b> ${radioHog.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.area')}:</b> ${radioAreaKm2} km²`;
			}
			tooltip.innerHTML = html;
			tooltip.style.display = 'block';
			tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
			tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
		});
		map.on('mouseleave', 'corrientes-buildings-3d', () => {
			leaveTimeout = setTimeout(() => {
				if (!lassoActive) map.getCanvas().style.cursor = '';
				tooltip.style.display = 'none';
			}, 80);
		});

		// Chaco / Formosa / BR buildings tooltip. Chaco+Formosa carry census
		// (redcode → radio block); the BR territories are footprints only, so
		// the redcode block simply doesn't render (no redcode prop on the tile).
		for (const blayer of [
			'chaco-buildings-3d', 'formosa-buildings-3d',
			'parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d'
		]) {
			map.on('mousemove', blayer, (e) => {
				if (lassoActive) return;
				if (leaveTimeout) { clearTimeout(leaveTimeout); leaveTimeout = null; }
				map.getCanvas().style.cursor = 'pointer';
				const p = e.features![0].properties!;

				const pers = parseInt(p.est_personas) || 0;
				const h = p.best_height_m != null ? parseFloat(p.best_height_m).toFixed(1) : '?';
				const a = p.area_m2 != null ? Math.round(p.area_m2).toLocaleString() : '?';
				const redcode = p.redcode || null;
				const radioPop = parseInt(p.radio_personas) || 0;
				const radioDens = p.densidad_hab_km2 != null ? Math.round(p.densidad_hab_km2).toLocaleString() : '?';
				const radioViv = parseInt(p.radio_viviendas) || 0;
				const radioHog = parseInt(p.radio_hogares) || 0;
				const radioAreaKm2 = p.radio_area_km2 != null ? parseFloat(p.radio_area_km2).toFixed(1) : '?';

				let html = `<b style="color:#60a5fa">${i18n.t('tip.building')}</b> ${i18n.t('tip.height')} ${h} m | ${i18n.t('tip.area')} ${a} m²<br>` +
					`<b style="color:#60a5fa">${i18n.t('tip.estPersons')}</b> <span style="color:#60a5fa;font-weight:600">${pers}</span>`;
				if (redcode) {
					html += `<br><span style="color:#a3a3a3">───</span><br>` +
						`<b style="color:#d4d4d4">${i18n.t('tip.radio')}</b> <span style="color:#d4d4d4">${redcode}</span><br>` +
						`<b style="color:#d4d4d4">${i18n.t('tip.pop')}</b> ${radioPop.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('tip.density')}</b> ${radioDens} hab/km²<br>` +
						`<b style="color:#d4d4d4">${i18n.t('label.dwellings')}:</b> ${radioViv.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.households')}:</b> ${radioHog.toLocaleString()} &nbsp; <b style="color:#d4d4d4">${i18n.t('label.area')}:</b> ${radioAreaKm2} km²`;
				}
				tooltip.innerHTML = html;
				tooltip.style.display = 'block';
				tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
				tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
			});
			map.on('mouseleave', blayer, () => {
				leaveTimeout = setTimeout(() => {
					if (!lassoActive) map.getCanvas().style.cursor = '';
					tooltip.style.display = 'none';
				}, 80);
			});
		}

		// Itapúa buildings tooltip (height + area only, no census data)
		map.on('mousemove', 'itapua-buildings-3d', (e) => {
			if (lassoActive) return;
			if (leaveTimeout) { clearTimeout(leaveTimeout); leaveTimeout = null; }
			map.getCanvas().style.cursor = 'pointer';
			const p = e.features![0].properties!;
			const h = p.best_height_m != null ? parseFloat(p.best_height_m).toFixed(1) : '?';
			const a = p.area_m2 != null ? Math.round(p.area_m2).toLocaleString() : '?';
			const res = p.is_residential ? 'residencial' : (p.subtype || 'no residencial');
			const dist = p.distrito || '';
			const est = p.est_personas > 0 ? ` | ~${p.est_personas} pers.` : '';
			tooltip.innerHTML = `<b style="color:#60a5fa">${i18n.t('tip.building')}</b> ${h} m | ${a} m\u00B2 | ${res}${est}${dist ? ` | ${formatDept(dist)}` : ''}`;
			tooltip.style.display = 'block';
			tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
			tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
		});
		map.on('mouseleave', 'itapua-buildings-3d', () => {
			leaveTimeout = setTimeout(() => {
				if (!lassoActive) map.getCanvas().style.cursor = '';
				tooltip.style.display = 'none';
			}, 80);
		});

		// Alto Paraná buildings tooltip (mirrors Itapúa)
		map.on('mousemove', 'alto_parana-buildings-3d', (e) => {
			if (lassoActive) return;
			if (leaveTimeout) { clearTimeout(leaveTimeout); leaveTimeout = null; }
			map.getCanvas().style.cursor = 'pointer';
			const p = e.features![0].properties!;
			const h = p.best_height_m != null ? parseFloat(p.best_height_m).toFixed(1) : '?';
			const a = p.area_m2 != null ? Math.round(p.area_m2).toLocaleString() : '?';
			const res = p.is_residential ? 'residencial' : (p.subtype || 'no residencial');
			const dist = p.distrito || '';
			const est = p.est_personas > 0 ? ` | ~${p.est_personas} pers.` : '';
			tooltip.innerHTML = `<b style="color:#60a5fa">${i18n.t('tip.building')}</b> ${h} m | ${a} m² | ${res}${est}${dist ? ` | ${formatDept(dist)}` : ''}`;
			tooltip.style.display = 'block';
			tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
			tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
		});
		map.on('mouseleave', 'alto_parana-buildings-3d', () => {
			leaveTimeout = setTimeout(() => {
				if (!lassoActive) map.getCanvas().style.cursor = '';
				tooltip.style.display = 'none';
			}, 80);
		});
		// Alto Paraná district hover: pointer cursor (badge text is Phase 4 polish)
		map.on('mouseenter', 'alto_parana-district-fill', () => { if (!lassoActive) map.getCanvas().style.cursor = 'pointer'; });
		map.on('mouseleave', 'alto_parana-district-fill', () => { if (!lassoActive) map.getCanvas().style.cursor = ''; });

		// Itapúa area hover badge (district layer + regional hex layer)
		const dispatchItapuaEnter = () => container.dispatchEvent(new CustomEvent('itapua-area-enter', { bubbles: true }));
		const dispatchItapuaLeave = () => container.dispatchEvent(new CustomEvent('itapua-area-leave', { bubbles: true }));
		map.on('mouseenter', 'itapua-district-fill', dispatchItapuaEnter);
		map.on('mouseleave', 'itapua-district-fill', dispatchItapuaLeave);
		map.on('mouseenter', 'regional-hex-fill', () => {
			dispatchItapuaEnter();
			if (!lassoActive) map.getCanvas().style.cursor = 'pointer';
		});
		map.on('mouseleave', 'regional-hex-fill', () => {
			dispatchItapuaLeave();
			if (!lassoActive) map.getCanvas().style.cursor = '';
		});

		// Click-to-select/deselect radio (multi-select)
		map.on('click', 'buildings-3d', (e) => {
			if (lassoActive) return;
			if (mapStore.activeHexLayer) return;
			if (catastroClickMode !== 'none') return; // catastro-fill handler handles it
			const redcode = e.features![0].properties!.redcode;
			if (!redcode) return;

			if (mapStore.hasRadio(redcode)) {
				container.dispatchEvent(new CustomEvent('radio-deselect', { bubbles: true, detail: { redcode } }));
			} else {
				// Query all visible buildings for this redcode
				const canvas = map.getCanvas();
				const allFeatures = map.queryRenderedFeatures(
					[[0, 0], [canvas.width, canvas.height]],
					{ layers: ['buildings-3d'] }
				);
				const selected: Record<string, any>[] = [];
				const seen = new Set<string>();
				for (const f of allFeatures) {
					const id = f.properties?.gba_id;
					if (f.properties?.redcode !== redcode || seen.has(id)) continue;
					seen.add(id);
					selected.push(f.properties!);
				}

				container.dispatchEvent(new CustomEvent('radio-select', {
					bubbles: true,
					detail: { redcode, selected, census: e.features![0].properties! }
				}));
			}
		});

		// Corrientes buildings: click-to-select radio (same behavior as buildings-3d)
		map.on('click', 'corrientes-buildings-3d', (e) => {
			if (lassoActive) return;
			if (mapStore.activeHexLayer) return;
			if (catastroClickMode !== 'none') return;
			const redcode = e.features![0].properties!.redcode;
			if (!redcode) return;

			if (mapStore.hasRadio(redcode)) {
				container.dispatchEvent(new CustomEvent('radio-deselect', { bubbles: true, detail: { redcode } }));
			} else {
				const canvas = map.getCanvas();
				const allFeatures = map.queryRenderedFeatures(
					[[0, 0], [canvas.width, canvas.height]],
					{ layers: ['corrientes-buildings-3d'] }
				);
				const selected: Record<string, any>[] = [];
				const seen = new Set<string | number>();
				for (const f of allFeatures) {
					const id = f.id ?? `${f.properties?.area_m2}_${f.properties?.est_personas}`;
					if (f.properties?.redcode !== redcode || seen.has(id)) continue;
					seen.add(id as string | number);
					selected.push(f.properties!);
				}
				container.dispatchEvent(new CustomEvent('radio-select', {
					bubbles: true,
					detail: { redcode, selected, census: e.features![0].properties! }
				}));
			}
		});

		// Chaco / Formosa / Brazil buildings: click-to-select radio/setor (mirror Corrientes).
		for (const blayer of ['chaco-buildings-3d', 'formosa-buildings-3d',
		                      'parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d']) {
			map.on('click', blayer, (e) => {
				if (lassoActive) return;
				if (mapStore.activeHexLayer) return;
				if (catastroClickMode !== 'none') return;
				const redcode = e.features![0].properties!.redcode;
				if (!redcode) return;

				if (mapStore.hasRadio(redcode)) {
					container.dispatchEvent(new CustomEvent('radio-deselect', { bubbles: true, detail: { redcode } }));
				} else {
					const canvas = map.getCanvas();
					const allFeatures = map.queryRenderedFeatures(
						[[0, 0], [canvas.width, canvas.height]],
						{ layers: [blayer] }
					);
					const selected: Record<string, any>[] = [];
					const seen = new Set<string | number>();
					for (const f of allFeatures) {
						const id = f.id ?? `${f.properties?.area_m2}_${f.properties?.est_personas}`;
						if (f.properties?.redcode !== redcode || seen.has(id)) continue;
						seen.add(id as string | number);
						selected.push(f.properties!);
					}
					container.dispatchEvent(new CustomEvent('radio-select', {
						bubbles: true,
						detail: { redcode, selected, census: e.features![0].properties! }
					}));
				}
			});
		}

		// Itapúa buildings: click → district profile. PY has no radio census, so
		// the district is the finest census unit available — this mirrors the AR
		// building→radio click, reusing the district-select machinery
		// (DistrictComparisonChart + fetchDistrictEnrichment: DGEEC pop/hog + NBI).
		map.on('click', 'itapua-buildings-3d', (e) => {
			if (lassoActive || mapStore.activeHexLayer) return;
			const p = e.features![0].properties!;
			const distrito = p.distrito;
			if (!distrito) return;
			const personas = p.distrito_pop ?? p.est_personas ?? 0;
			const event = mapStore.hasDistrict(distrito) ? 'district-deselect' : 'district-select';
			container.dispatchEvent(new CustomEvent(event, { bubbles: true, detail: { distrito, personas, territory: 'itapua_py' } }));
		});

		// Alto Paraná buildings: click → district profile (mirrors Itapúa).
		map.on('click', 'alto_parana-buildings-3d', (e) => {
			if (lassoActive || mapStore.activeHexLayer) return;
			const p = e.features![0].properties!;
			const distrito = p.distrito;
			if (!distrito) return;
			const personas = p.distrito_pop ?? p.est_personas ?? 0;
			const event = mapStore.hasDistrict(distrito) ? 'district-deselect' : 'district-select';
			container.dispatchEvent(new CustomEvent(event, { bubbles: true, detail: { distrito, personas, territory: 'alto_parana_py' } }));
		});

		// New PY territories buildings: click → district profile (mirrors Itapúa/Alto Paraná)
		const PY_BUILDING_LAYERS: [string, string][] = [
			['concepcion-buildings-3d', 'concepcion_py'],
			['san_pedro-buildings-3d', 'san_pedro_py'],
			['cordillera-buildings-3d', 'cordillera_py'],
			['guaira-buildings-3d', 'guaira_py'],
			['caaguazu-buildings-3d', 'caaguazu_py'],
			['caazapa-buildings-3d', 'caazapa_py'],
			['misiones-buildings-3d', 'misiones_py'],
			['paraguari-buildings-3d', 'paraguari_py'],
			['central-buildings-3d', 'central_py'],
			['neembucu-buildings-3d', 'neembucu_py'],
			['amambay-buildings-3d', 'amambay_py'],
			['canindeyu-buildings-3d', 'canindeyu_py'],
			['presidente_hayes-buildings-3d', 'presidente_hayes_py'],
			['boqueron-buildings-3d', 'boqueron_py'],
			['alto_paraguay-buildings-3d', 'alto_paraguay_py'],
		];
		for (const [layerId, terrId] of PY_BUILDING_LAYERS) {
			map.on('click', layerId, (e) => {
				if (lassoActive || mapStore.activeHexLayer) return;
				const p = e.features![0].properties!;
				const distrito = p.distrito;
				if (!distrito) return;
				const personas = p.distrito_pop ?? p.est_personas ?? 0;
				const event = mapStore.hasDistrict(distrito) ? 'district-deselect' : 'district-select';
				container.dispatchEvent(new CustomEvent(event, { bubbles: true, detail: { distrito, personas, territory: terrId } }));
			});
		}

		// Phase 2b: district polygon click → load that district's hexes for the active analysis.
		// Covers Itapúa, Alto Paraná, and all new PY departments.
		const PY_DISTRICT_LAYERS: Record<string, string> = {
			'itapua-district-fill':              'itapua_py',
			'alto_parana-district-fill':         'alto_parana_py',
			'concepcion_py-district-fill':       'concepcion_py',
			'san_pedro_py-district-fill':        'san_pedro_py',
			'cordillera_py-district-fill':       'cordillera_py',
			'guaira_py-district-fill':           'guaira_py',
			'caaguazu_py-district-fill':         'caaguazu_py',
			'caazapa_py-district-fill':          'caazapa_py',
			'misiones_py-district-fill':         'misiones_py',
			'paraguari_py-district-fill':        'paraguari_py',
			'central_py-district-fill':          'central_py',
			'neembucu_py-district-fill':         'neembucu_py',
			'amambay_py-district-fill':          'amambay_py',
			'canindeyu_py-district-fill':        'canindeyu_py',
			'presidente_hayes_py-district-fill': 'presidente_hayes_py',
			'boqueron_py-district-fill':         'boqueron_py',
			'alto_paraguay_py-district-fill':    'alto_paraguay_py',
		};
		for (const lyr of Object.keys(PY_DISTRICT_LAYERS)) {
			const terr = PY_DISTRICT_LAYERS[lyr];
			map.on('mouseenter', lyr, () => { if (!lassoActive) map.getCanvas().style.cursor = 'pointer'; });
			map.on('mouseleave', lyr, () => { if (!lassoActive) map.getCanvas().style.cursor = ''; });
			map.on('click', lyr, (e) => {
				if (lassoActive) return;
				const name = e.features![0].properties!.district;
				if (!name) return;
				container.dispatchEvent(new CustomEvent('dept-map-select', {
					bubbles: true,
					detail: { name, territory: terr }
				}));
			});
		}

		// Click on hexagon: emit hex-select event
		map.on('click', 'hex-fill', (e) => {
			if (lassoActive) return;
			const h3index = e.features![0].properties!.h3index;
			if (!h3index) return;
			container.dispatchEvent(new CustomEvent('hex-select', {
				bubbles: true,
				detail: { h3index, properties: e.features![0].properties! }
			}));
		});

		map.on('click', 'compare-hex-fill', (e) => {
			if (lassoActive) return;
			const h3index = e.features![0].properties!.h3index;
			if (!h3index) return;
			container.dispatchEvent(new CustomEvent('compare-hex-select', {
				bubbles: true,
				detail: { h3index, properties: e.features![0].properties! }
			}));
		});

		// Click on regional (Itapúa) hex in regional mode
		map.on('click', 'regional-hex-fill', (e) => {
			if (lassoActive) return;
			const h3index = e.features![0].properties!.h3index;
			if (!h3index) return;
			container.dispatchEvent(new CustomEvent('regional-hex-select', {
				bubbles: true,
				detail: { h3index }
			}));
		});

		// Click on selected hex border (thick line intercepts before hex-fill)
		map.on('click', 'hex-selected', (e) => {
			if (lassoActive) return;
			const h3index = e.features![0].properties!.h3index;
			if (!h3index) return;
			container.dispatchEvent(new CustomEvent('hex-select', {
				bubbles: true,
				detail: { h3index, properties: e.features![0].properties! }
			}));
		});

		// Hex hover tooltip
		map.on('mousemove', 'hex-fill', (e) => {
			if (lassoActive) return;
			const p = e.features![0].properties!;
			if (!p.h3index) return;
			map.getCanvas().style.cursor = 'pointer';

			const titleLine = hexLayerTitle ? `<div style="color:rgba(255,255,255,0.5);font-size:9px;margin-bottom:2px">${hexLayerTitle}</div>` : '';

			let valueLine: string;
			if (p.nodata === true || p.nodata === 'true') {
				valueLine = `<span style="color:#94a3b8;font-weight:600;font-style:italic">${i18n.t('legend.noData')}</span>`;
			} else if (hexLayerIsCategorical && p.type_label) {
				valueLine = `<span style="color:#e2e8f0;font-weight:600">${p.type_label}</span>`;
			} else {
				const score = p.value != null ? Number(p.value).toFixed(1) : '—';
				// Physical unit of the primary variable (tC/ha, min, %…), set per layer.
				// Empty → no suffix. Replaces the old hardcoded "/100", which mislabeled
				// every raw layer as a 0-100 score.
				const suffix = hexLayerUnit
					? `<span style="color:rgba(255,255,255,0.4);font-size:9px"> ${hexLayerUnit}</span>`
					: '';
				valueLine = `<span style="color:#e2e8f0;font-weight:600">${score}</span>${suffix}`;
			}

			tooltip.innerHTML = `${titleLine}${valueLine}`;
			tooltip.style.display = 'block';
			tooltip.style.left = (e.originalEvent.clientX + 14) + 'px';
			tooltip.style.top = (e.originalEvent.clientY + 14) + 'px';
		});

		map.on('mouseleave', 'hex-fill', () => {
			if (!lassoActive) map.getCanvas().style.cursor = '';
			tooltip.style.display = 'none';
		});

		map.on('mousemove', 'compare-hex-fill', () => {
			if (!lassoActive) map.getCanvas().style.cursor = 'pointer';
		});
		map.on('mouseleave', 'compare-hex-fill', () => {
			if (!lassoActive) map.getCanvas().style.cursor = '';
		});

		// General mousemove: show pointer over blank territory areas (Option A nav hint)
		map.on('mousemove', (e) => {
			if (lassoActive) return;
			const canvas = map.getCanvas();
			if (canvas.style.cursor !== '') return; // specific layer handler already set it
			const { lat, lng } = e.lngLat;
			const inTerritory = isInsideItapua(lat, lng) || isInsideMisiones(lat, lng) || isInsideCorrientes(lat, lng) || isInsideAltoParana(lat, lng);
			canvas.style.cursor = inTerritory ? 'pointer' : '';
		});

		// General click: switch territory scope when clicking blank area inside a territory
		const TERRITORY_LAYERS = ['hex-fill', 'compare-hex-fill', 'regional-hex-fill',
			'buildings-3d', 'corrientes-buildings-3d', 'itapua-buildings-3d', 'alto_parana-buildings-3d',
			'chaco-buildings-3d', 'formosa-buildings-3d',
			'parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d',
			'buildings-flat', 'province-fill', 'itapua-district-fill', 'alto_parana-district-fill'];
		map.on('click', (e) => {
			if (lassoActive) return;
			const activeLayers = TERRITORY_LAYERS.filter(l => map.getLayer(l));
			if (activeLayers.length > 0 && map.queryRenderedFeatures(e.point, { layers: activeLayers }).length > 0) return;
			const { lat, lng } = e.lngLat;
			let territory: string | null = null;
			if (isInsideItapua(lat, lng)) territory = 'itapua_py';
			else if (isInsideAltoParana(lat, lng)) territory = 'alto_parana_py';
			else if (isInsideMisiones(lat, lng)) territory = 'misiones';
			else if (isInsideCorrientes(lat, lng)) territory = 'corrientes';
			if (territory) {
				container.dispatchEvent(new CustomEvent('territory-map-select', {
					bubbles: true,
					detail: { territory }
				}));
			}
		});

	}

	export function setHexLayerInfo(title: string, isCategorical: boolean, unit: string = '') {
		hexLayerTitle = title;
		hexLayerIsCategorical = isCategorical;
		hexLayerUnit = unit;
	}

	export function flyToInit() {
		map?.flyTo({ ...MAP_PROVINCE, duration: 1200 });
	}

	export function getRadioGeometry(redcode: string): any | null {
		if (!map) return null;
		const features = map.querySourceFeatures('radios', {
			sourceLayer: 'radios',
			filter: ['==', ['get', 'redcode'], redcode] as any
		});
		if (features.length === 0) return null;
		return features[0].geometry;
	}

	export function setPitch(p: number) {
		map?.easeTo({ pitch: p, duration: 200 });
	}



	// activeTerritoryId → its 3D buildings layer id. Territories not listed
	// (misiones) fall back to the base 'buildings-3d'.
	const TERRITORY_BUILDINGS_LAYER: Record<string, string> = {
		corrientes: 'corrientes-buildings-3d',
		itapua_py: 'itapua-buildings-3d',
		alto_parana_py: 'alto_parana-buildings-3d',
		chaco: 'chaco-buildings-3d',
		formosa: 'formosa-buildings-3d',
		parana_br: 'parana_br-buildings-3d',
		santa_catarina_br: 'santa_catarina_br-buildings-3d',
		rio_grande_sul_br: 'rio_grande_sul_br-buildings-3d',
		// Paraguay departments
		concepcion_py: 'concepcion-buildings-3d',
		san_pedro_py: 'san_pedro-buildings-3d',
		cordillera_py: 'cordillera-buildings-3d',
		guaira_py: 'guaira-buildings-3d',
		caaguazu_py: 'caaguazu-buildings-3d',
		caazapa_py: 'caazapa-buildings-3d',
		misiones_py: 'misiones-buildings-3d',
		paraguari_py: 'paraguari-buildings-3d',
		central_py: 'central-buildings-3d',
		neembucu_py: 'neembucu-buildings-3d',
		amambay_py: 'amambay-buildings-3d',
		canindeyu_py: 'canindeyu-buildings-3d',
		presidente_hayes_py: 'presidente_hayes-buildings-3d',
		boqueron_py: 'boqueron-buildings-3d',
		alto_paraguay_py: 'alto_paraguay-buildings-3d',
	};
	const ALL_BUILDINGS_LAYERS = ['buildings-3d', ...Object.values(TERRITORY_BUILDINGS_LAYER)];

	function showBuildingsForActiveTerritory() {
		const isItapua = activeTerritoryId === 'itapua_py';
		const isAltoParana = activeTerritoryId === 'alto_parana_py';
		const layer = TERRITORY_BUILDINGS_LAYER[activeTerritoryId] ?? 'buildings-3d';
		const heightColored = isItapua || isAltoParana;
		const opacity = heightColored ? 0.92 : 0.85;
		const colorExpr = heightColored ? mapStore.getHeightColorExpr() : mapStore.getColorExpr();
		if (map?.getLayer(layer)) {
			map.setLayoutProperty(layer, 'visibility', 'visible');
			map.setPaintProperty(layer, 'fill-extrusion-color', colorExpr as any);
			map.setPaintProperty(layer, 'fill-extrusion-opacity', opacity);
		}
		updatePlantationsVisibility();
	}

	// Show the plantations overlay only for the active AR territory, and only when the
	// forestry_aptitude layer is active and the user toggled it on. All others hidden.
	function updatePlantationsVisibility() {
		if (!map || !map.isStyleLoaded()) return;
		const forestryActive = mapStore.activeHexLayer === 'forestry_aptitude';
		// DNDFI `depto` is upper-case; the app's selected dept name matches once upcased.
		const dept = (mapStore.plantationsDept || '').toUpperCase();
		for (const t of PLANTATION_TERRITORIES) {
			const vis = (forestryActive && mapStore.plantationsVisible && !!dept && activeTerritoryId === t);
			for (const suffix of ['fill', 'line']) {
				const id = `${t}-plantations-${suffix}`;
				if (!map.getLayer(id)) continue;
				if (vis) {
					// Show only the selected department's plantations (per-dept, any zoom).
					map.setFilter(id, ['==', ['get', 'depto'], dept]);
					map.setLayoutProperty(id, 'visibility', 'visible');
					// Lift above the hex choropleth (but below labels) so the dense Misiones
					// score layer doesn't bury the overlay.
					map.moveLayer(id, firstSymbolId);
				} else {
					map.setLayoutProperty(id, 'visibility', 'none');
				}
			}
		}
	}

	// React to layer-activation / toggle changes (territory changes call it directly).
	$effect(() => {
		mapStore.activeHexLayer;
		mapStore.plantationsVisible;
		mapStore.plantationsDept;
		updatePlantationsVisibility();
	});

	export function updateColorExpr() {
		const colorExpr = mapStore.getColorExpr() as any;
		// Default (AR-style, est_personas) color expr — applies to Misiones,
		// Corrientes, Chaco, Formosa and the BR footprint layers. Itapúa / Alto
		// Paraná use the height-colored variant and are handled separately.
		for (const l of [
			'buildings-3d', 'corrientes-buildings-3d',
			'chaco-buildings-3d', 'formosa-buildings-3d',
			'parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d'
		]) {
			if (map?.getLayer(l)) {
				map.setPaintProperty(l, 'fill-extrusion-color', colorExpr);
			}
		}
	}

	// activeTerritoryId → its radio-highlight building-outline layer. Census
	// territories only (AR). Misiones uses the base 'radio-highlight'.
	const TERRITORY_RADIO_HIGHLIGHT: Record<string, string> = {
		corrientes:        'radio-highlight-corrientes',
		chaco:             'radio-highlight-chaco',
		formosa:           'radio-highlight-formosa',
		parana_br:         'radio-highlight-parana_br',
		santa_catarina_br: 'radio-highlight-santa_catarina_br',
		rio_grande_sul_br: 'radio-highlight-rio_grande_sul_br',
	};
	const ALL_RADIO_HIGHLIGHT = ['radio-highlight', ...Object.values(TERRITORY_RADIO_HIGHLIGHT)];

	export function setRadioHighlight(radios: Array<{redcode: string, color: string}>) {
		const activeLayer = TERRITORY_RADIO_HIGHLIGHT[activeTerritoryId] ?? 'radio-highlight';
		const emptyFilter: any = ['==', ['get', 'redcode'], ''];
		// Clear every other radio-highlight layer so a stale outline never lingers
		// when switching between AR census territories.
		for (const l of ALL_RADIO_HIGHLIGHT) {
			if (l !== activeLayer && map?.getLayer(l)) map.setFilter(l, emptyFilter);
		}
		if (!map?.getLayer(activeLayer)) return;
		if (radios.length === 0) {
			map.setFilter(activeLayer, emptyFilter);
			if (map.getLayer('selected-fill')) map.setFilter('selected-fill', emptyFilter);
			if (map.getLayer('selected-line')) map.setFilter('selected-line', emptyFilter);
		} else {
			const redcodes = radios.map(r => r.redcode);
			const matchExpr: any[] = ['match', ['get', 'redcode']];
			for (const r of radios) {
				matchExpr.push(r.redcode, r.color);
			}
			matchExpr.push('#60a5fa'); // fallback
			map.setPaintProperty(activeLayer, 'line-color', matchExpr);
			map.setPaintProperty(activeLayer, 'line-width', 4.5);
			map.setFilter(activeLayer, ['in', ['get', 'redcode'], ['literal', redcodes]]);
			// Always highlight census radio polygons (selected-fill/selected-line use the
			// radios PMTile which covers all AR census territories — Mis, Cor, Chaco, Formosa).
			const polyFilter: any = ['in', ['get', 'redcode'], ['literal', redcodes]];
			if (map.getLayer('selected-fill')) {
				map.setPaintProperty('selected-fill', 'fill-color', matchExpr);
				map.setPaintProperty('selected-fill', 'fill-opacity', 0.30);
				map.setFilter('selected-fill', polyFilter);
			}
			if (map.getLayer('selected-line')) {
				map.setPaintProperty('selected-line', 'line-color', matchExpr);
				map.setPaintProperty('selected-line', 'line-width', 2.5);
				map.setFilter('selected-line', polyFilter);
			}
		}
	}

	export function clearRadioHighlight() {
		if (!map) return;
		const emptyFilter: any = ['==', ['get', 'redcode'], ''];
		for (const l of ALL_RADIO_HIGHLIGHT) {
			if (map.getLayer(l)) map.setFilter(l, emptyFilter);
		}
		map.setFilter('selected-fill', emptyFilter);
		map.setFilter('selected-line', emptyFilter);
	}

	export function setDistrictHighlight(districts: Array<{distrito: string, color: string, territory?: string}>) {
		if (!map) return;
		const emptyFilter: any = ['==', ['get', 'district'], ''];
		// Apply highlight PER territory so Itapúa + Alto Paraná selections
		// coexist (cross-territory district comparison on the base map).
		// A territory with zero selected districts gets cleared — but the
		// OTHER territory's highlight is never wiped.
		// Value = ACTUAL layer-id prefix. Itapúa/Alto Paraná layers are created
		// stripped (lines ~417/476) → stripped prefix. The other 15 PY depts are
		// created in the PY_TERR_LIST loop with the FULL `_py` id (distId = tid)
		// → their prefix MUST keep `_py`, else getLayer() misses and the highlight
		// silently never renders.
		const TERR_PREFIX: Record<string, string> = {
			itapua_py: 'itapua', alto_parana_py: 'alto_parana',
			concepcion_py: 'concepcion_py', san_pedro_py: 'san_pedro_py',
			cordillera_py: 'cordillera_py', guaira_py: 'guaira_py',
			caaguazu_py: 'caaguazu_py', caazapa_py: 'caazapa_py',
			misiones_py: 'misiones_py', paraguari_py: 'paraguari_py',
			central_py: 'central_py', neembucu_py: 'neembucu_py',
			amambay_py: 'amambay_py', canindeyu_py: 'canindeyu_py',
			presidente_hayes_py: 'presidente_hayes_py', boqueron_py: 'boqueron_py',
			alto_paraguay_py: 'alto_paraguay_py',
		};
		for (const [terrId, prefix] of Object.entries(TERR_PREFIX)) {
			const fillId = `${prefix}-district-selected-fill`;
			const lineId = `${prefix}-district-selected-line`;
			if (!map.getLayer(fillId)) continue;
			const ds = districts.filter(d => (d.territory ?? 'itapua_py') === terrId);
			if (ds.length === 0) {
				map.setFilter(fillId, emptyFilter);
				map.setFilter(lineId, emptyFilter);
				map.setLayoutProperty(fillId, 'visibility', 'none');
				map.setLayoutProperty(lineId, 'visibility', 'none');
				continue;
			}
			const names = ds.map(d => d.distrito);
			const matchExpr: any[] = ['match', ['get', 'district']];
			for (const d of ds) matchExpr.push(d.distrito, d.color);
			matchExpr.push('#60a5fa'); // fallback
			// setDistrictHighlight solely owns -selected- visibility: show the
			// clicked district's polygon (parallels the AR radio highlight) so
			// the petal/profile data is visibly tied to its district unit.
			map.setLayoutProperty(fillId, 'visibility', 'visible');
			map.setLayoutProperty(lineId, 'visibility', 'visible');
			map.setPaintProperty(fillId, 'fill-color', matchExpr);
			map.setPaintProperty(fillId, 'fill-opacity', 0.45);
			map.setFilter(fillId, ['in', ['get', 'district'], ['literal', names]]);
			map.setPaintProperty(lineId, 'line-color', matchExpr);
			map.setFilter(lineId, ['in', ['get', 'district'], ['literal', names]]);
		}
	}

	export function flyToCoords(lat: number, lng: number, zoom?: number) {
		map?.flyTo({
			center: [lng, lat],
			zoom: zoom || 12,
			pitch: 50,
			duration: 1500
		});
	}

	export function flyToBbox(bbox: [number, number, number, number]) {
		// bbox: [W, S, E, N]
		map?.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], {
			padding: 40,
			pitch: 50,
			duration: 1500,
			maxZoom: 10
		});
	}

	export function fitBoundsDept(bbox: [number, number, number, number]) {
		// bbox: [minLng, minLat, maxLng, maxLat] — fits tightly to actual dept hexagons
		map?.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], {
			padding: 20,
			pitch: 50,
			duration: 1500,
		});
	}

	export function updateDeptHighlights(
		primary: [number, number, number, number] | null,
		compare: [number, number, number, number] | null
	) {
		const src = map?.getSource('dept-highlights') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		const features: any[] = [];
		if (primary) {
			const [w, s, e, n] = primary;
			features.push({
				type: 'Feature',
				properties: { color: '#60a5fa' },
				geometry: { type: 'Polygon', coordinates: [[[w,s],[e,s],[e,n],[w,n],[w,s]]] }
			});
		}
		if (compare) {
			const [w, s, e, n] = compare;
			features.push({
				type: 'Feature',
				properties: { color: '#f59e0b' },
				geometry: { type: 'Polygon', coordinates: [[[w,s],[e,s],[e,n],[w,n],[w,s]]] }
			});
		}
		src.setData({ type: 'FeatureCollection', features });
		// Show border only in compare mode (both depts present); hide in single-dept mode
		const compareMode = !!(primary && compare);
		if (map.getLayer('dept-highlight-line')) {
			map.setPaintProperty('dept-highlight-line', 'line-opacity',
				compareMode ? ['interpolate', ['linear'], ['zoom'], 4, 0.8, 10, 0.5, 14, 0.25] : 0
			);
		}
		if (map.getLayer('dept-highlight-fill')) {
			map.setPaintProperty('dept-highlight-fill', 'fill-opacity', compareMode ? 0.06 : 0);
		}
	}

	export function setDeptOutline(feature: any | null) {
		const src = map?.getSource('dept-outline') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		src.setData({
			type: 'FeatureCollection',
			features: feature ? [feature] : []
		});
		// dept-outline-line is created before the hex layers, so the choropleth
		// (fill-opacity 0.78) renders on top and hides the boundary. Lift it above
		// the hexes whenever a dept is selected so the border stays visible.
		if (feature && map?.getLayer('dept-outline-line')) map.moveLayer('dept-outline-line');
	}

	export function setCompareDeptOutline(feature: any | null) {
		const src = map?.getSource('compare-dept-outline') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		src.setData({
			type: 'FeatureCollection',
			features: feature ? [feature] : []
		});
		if (feature && map?.getLayer('compare-dept-outline-line')) map.moveLayer('compare-dept-outline-line');
	}

	function applyTerritoryVisibility() {
		if (!map) return;

		if (regionalModeActive) {
			// Regional mode: show ALL territory buildings simultaneously (all 3 PMTiles pre-loaded).
			// Fog masks and borders are managed by setRegionalMapMode().
			// All territories' buildings colored by est_personas (dasymetric
			// population) — consistent meaning across the frontier, not height.
			const colorExprDefault = mapStore.getColorExpr();
			const colorExprItapua = colorExprDefault;
			for (const l of ALL_BUILDINGS_LAYERS) {
				if (map.getLayer(l)) {
					map.setLayoutProperty(l, 'visibility', 'visible');
					const expr = (l === 'itapua-buildings-3d' || l === 'alto_parana-buildings-3d') ? colorExprItapua : colorExprDefault;
					map.setPaintProperty(l, 'fill-extrusion-color', expr as any);
				}
			}
			return;
		}

		const isMisiones = activeTerritoryId === 'misiones';
		const isItapua = activeTerritoryId === 'itapua_py';
		const isCorrientes = activeTerritoryId === 'corrientes';
		const isAltoParana = activeTerritoryId === 'alto_parana_py';

		// Census radios retired from the base/general view (legacy census-centric
		// model). Department selection is not radio-based — never auto-show them.
		const showProvinceRadios = false;
		for (const layerId of ['province-fill', 'province-line']) {
			if (map.getLayer(layerId)) {
				map.setLayoutProperty(layerId, 'visibility', showProvinceRadios ? 'visible' : 'none');
				if (showProvinceRadios) {
					map.setFilter(layerId, ['==', ['get', 'codprov'], isCorrientes ? '18' : '54']);
				}
			}
		}
		// Misiones-only: fog mask + province border polygon
		for (const layerId of ['mask-fill', 'province-border']) {
			if (map.getLayer(layerId)) {
				map.setLayoutProperty(layerId, 'visibility', isMisiones ? 'visible' : 'none');
			}
		}
		// Itapúa fog mask
		if (map.getLayer('itapua-mask-fill')) {
			map.setLayoutProperty('itapua-mask-fill', 'visibility', isItapua ? 'visible' : 'none');
		}
		// Corrientes fog mask (border stays always-visible like Mis/Ita to signal coverage)
		if (map.getLayer('corrientes-mask-fill')) {
			map.setLayoutProperty('corrientes-mask-fill', 'visibility', isCorrientes ? 'visible' : 'none');
		}
		// Alto Paraná fog mask (border stays always-visible like Mis/Ita to signal coverage)
		if (map.getLayer('alto_parana-mask-fill')) {
			map.setLayoutProperty('alto_parana-mask-fill', 'visibility', isAltoParana ? 'visible' : 'none');
		}
		// PY district polygons: show BOTH Itapúa + Alto Paraná whenever any
		// PY territory is active, so districts of the two can be selected and
		// compared together on the base map (no territory switch needed).
		// PY district fill/line = Phase-2 dept picker (setDeptPickerVisible).
		// The -selected- highlight layers are owned solely by
		// setDistrictHighlight (shown on a building→district click) — do NOT
		// force them here, or the click-highlight never renders.

		// Buildings: show only the active territory's layer, hide the others
		const activeLayer = TERRITORY_BUILDINGS_LAYER[activeTerritoryId] ?? 'buildings-3d';
		const otherLayers = ALL_BUILDINGS_LAYERS.filter(l => l !== activeLayer);
		for (const l of otherLayers) {
			if (map.getLayer(l)) map.setLayoutProperty(l, 'visibility', 'none');
		}
		if (map.getLayer(activeLayer)) {
			map.setLayoutProperty(activeLayer, 'visibility', 'visible');
			const colorExpr = mapStore.getColorExpr();
			map.setPaintProperty(activeLayer, 'fill-extrusion-color', colorExpr as any);
		}
	}

	export function setActiveTerritory(territoryId: string) {
		activeTerritoryId = territoryId;
		applyTerritoryVisibility();
	}

	// EUDR mode: hide the Spatia territory map (buildings/masks/borders/province)
	// and show only the NOA+NEA province outlines. These are restored on exit by
	// applyTerritoryVisibility() — so ONLY layers it restores belong here.
	//
	// Deliberately EXCLUDED (do NOT add them back): the dept-picker layers
	// (ar-dept-fill/line + every *-district-fill/line) are owned reactively by
	// setDeptPickerVisible — its $effect already hides them while EUDR is active
	// (show=false when activeAnalysis.id==='eudr') and restores them on exit. The
	// selected-dept outlines (dept-outline-line / compare-dept-outline-line) are
	// data-driven via setDeptOutline (empty while no dept is selected, as in EUDR).
	// Hiding any of these here too made applyTerritoryVisibility the wrong owner and
	// left them stuck-hidden after leaving EUDR → dept/distrito clicks died.
	const EUDR_HIDDEN_LAYERS = [
		'buildings-3d', 'itapua-buildings-3d', 'corrientes-buildings-3d', 'alto_parana-buildings-3d',
		'chaco-buildings-3d', 'formosa-buildings-3d',
		'parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d',
		'mask-fill', 'itapua-mask-fill', 'corrientes-mask-fill', 'alto_parana-mask-fill',
		'province-border', 'corrientes-border', 'alto_parana-border', 'itapua-border',
		'chaco-border', 'formosa-border', 'parana_br-border', 'santa_catarina_br-border', 'rio_grande_sul_br-border',
		'province-fill', 'province-line',
	];

	// Basemap (CartoDB) admin lines come from a different source than our GADM
	// boundaries and visibly drift, producing a duplicate/blurry outline next to
	// our pink province borders. Hide them once after load — same effect EUDR mode
	// applied locally, now applied to regular mode too.
	function hideBasemapAdminLines() {
		if (!map) return;
		const ids = (map.getStyle().layers ?? [])
			.filter(l => /admin|boundary/i.test(l.id) && (l.type === 'line' || l.type === 'fill'))
			.map(l => l.id);
		for (const id of ids) {
			try { map.setLayoutProperty(id, 'visibility', 'none'); } catch {}
		}
	}

	export function setEudrMode(active: boolean) {
		if (!map) return;
		const apply = () => {
			const eudrLayers = ['eudr-provinces-line', 'eudr-admin2-line', 'eudr-admin2-fill', 'eudr-focus-line'];
			if (active) {
				for (const l of EUDR_HIDDEN_LAYERS) {
					if (map.getLayer(l)) map.setLayoutProperty(l, 'visibility', 'none');
				}
				hideBasemapAdminLines();
				for (const l of eudrLayers) {
					if (map.getLayer(l)) map.setLayoutProperty(l, 'visibility', 'visible');
				}
			} else {
				for (const l of eudrLayers) {
					if (map.getLayer(l)) map.setLayoutProperty(l, 'visibility', 'none');
				}
				applyTerritoryVisibility();
			}
		};
		if (map.isStyleLoaded()) apply(); else map.once('idle', apply);
	}

	function regionalProvinceClickHandler(e: any) {
		if (lassoActive || mapStore.activeHexLayer) return;
		const redcode = e.features?.[0]?.properties?.redcode;
		if (!redcode) return;
		if (mapStore.hasRadio(redcode)) {
			container.dispatchEvent(new CustomEvent('radio-deselect', { bubbles: true, detail: { redcode } }));
		} else {
			container.dispatchEvent(new CustomEvent('radio-select', {
				bubbles: true,
				detail: { redcode, selected: [], census: e.features![0].properties! }
			}));
		}
	}

	export function setRegionalMapMode(active: boolean) {
		regionalModeActive = active;
		if (!map) return;
		if (active) {
			// Hide all fog masks
			for (const id of ['mask-fill', 'itapua-mask-fill', 'corrientes-mask-fill', 'alto_parana-mask-fill']) {
				if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
			}
			// Show all territory borders
			for (const id of ['province-border', 'itapua-border', 'corrientes-border', 'alto_parana-border', 'chaco-border', 'formosa-border', 'parana_br-border', 'santa_catarina_br-border', 'rio_grande_sul_br-border']) {
				if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'visible');
			}
			// Census radios are a census/base-mode visual. Keep them hidden while a
			// hex analysis is active (e.g. the deforestation cold-open) so the general
			// view isn't covered by radio polygons before zooming to the department.
			// Census radios retired from the base/general view (legacy census-centric
			// model). Never auto-shown, including regional mode.
			for (const id of ['province-fill', 'province-line']) {
				if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
			}
			// Itapúa + Alto Paraná district outlines: visible in regional census/base
			// mode only — hidden while a hex analysis is active (cold-open clutter).
			// PY district fill/line = Phase-2 dept picker (setDeptPickerVisible).
			// The -selected- highlight layers are owned solely by
			// setDistrictHighlight (shown on a building→district click) — do NOT
			// force them here, or the click-highlight never renders.
			// Census-radio click selection retired (radios no longer shown).
			map.off('click', 'province-fill', regionalProvinceClickHandler);
		} else {
			map.off('click', 'province-fill', regionalProvinceClickHandler);
			// Full restore via standard territory visibility logic
			applyTerritoryVisibility();
		}
	}

	// Re-apply census-radio visibility after the active hex layer changes
	// (cold-open analysis set, analysis switched, or cleared back to census).
	export function refreshCensusVisibility() {
		if (!map) return;
		if (regionalModeActive) setRegionalMapMode(true);
		else applyTerritoryVisibility();
	}

	// Phase 2: show/hide the department-picker polygons (clickable selection
	// surface, shown when an analysis is active and no department is selected).
	// For PY territories, only show the active territory's district fill/line.
	// Lazy-fill the AR dept picker source (the 465KB boundary GeoJSON is deferred out
	// of the initial bundle). Idempotent; no-op until the import resolves.
	export async function loadArDeptSource() {
		await ensureArBoundaries();
		const feats = getArFeatures();
		if (!feats || !map) return;
		const src = map.getSource('ar-depts') as any;
		if (src?.setData) src.setData({ type: 'FeatureCollection', features: feats });
	}

	export function setDeptPickerVisible(visible: boolean) {
		if (!map) return;
		// AR departments (always shown together since they don't overlap)
		for (const id of ['ar-dept-fill', 'ar-dept-line']) {
			if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
		}
		// PY district layers: show only the active territory's layer.
		// Layer IDs match the srcId from the creation loop (full territory ID).
		// Exceptions: 'itapua' and 'alto_parana' were created manually before the loop.
		const allPyDistrictLayers = [
			'itapua-district-fill',              'itapua-district-line',
			'alto_parana-district-fill',         'alto_parana-district-line',
			'concepcion_py-district-fill',       'concepcion_py-district-line',
			'san_pedro_py-district-fill',        'san_pedro_py-district-line',
			'cordillera_py-district-fill',       'cordillera_py-district-line',
			'guaira_py-district-fill',           'guaira_py-district-line',
			'caaguazu_py-district-fill',         'caaguazu_py-district-line',
			'caazapa_py-district-fill',          'caazapa_py-district-line',
			'misiones_py-district-fill',         'misiones_py-district-line',
			'paraguari_py-district-fill',        'paraguari_py-district-line',
			'central_py-district-fill',          'central_py-district-line',
			'neembucu_py-district-fill',         'neembucu_py-district-line',
			'amambay_py-district-fill',          'amambay_py-district-line',
			'canindeyu_py-district-fill',        'canindeyu_py-district-line',
			'presidente_hayes_py-district-fill', 'presidente_hayes_py-district-line',
			'boqueron_py-district-fill',         'boqueron_py-district-line',
			'alto_paraguay_py-district-fill',    'alto_paraguay_py-district-line',
		];
		for (const id of allPyDistrictLayers) {
			if (!map.getLayer(id)) continue;
			const isActiveTerrLayer = id.startsWith(activeTerritoryId);
			map.setLayoutProperty(id, 'visibility', (visible && isActiveTerrLayer) ? 'visible' : 'none');
		}
	}

	// ── Lens opportunity glow layers ─────────────────────────────────────────

	export function setOpportunityGlow(redcodes: string[], color: string) {
		if (!map) return;

		const filter: any = redcodes.length > 0
			? ['in', ['get', 'redcode'], ['literal', redcodes]]
			: ['==', ['get', 'redcode'], ''];

		// Layers are pre-created on map load — always update
		map.setPaintProperty('opportunity-fill', 'fill-color', color);
		map.setPaintProperty('opportunity-fill', 'fill-opacity', 0.25);
		map.setFilter('opportunity-fill', filter);

		map.setPaintProperty('opportunity-line', 'line-color', color);
		map.setFilter('opportunity-line', filter);
	}

	export function clearOpportunityGlow() {
		if (!map) return;
		const emptyFilter: any = ['==', ['get', 'redcode'], ''];
		if (map.getLayer('opportunity-fill')) {
			map.setFilter('opportunity-fill', emptyFilter);
		}
		if (map.getLayer('opportunity-line')) {
			map.setFilter('opportunity-line', emptyFilter);
		}
	}

	export function flyToProvince() {
		map?.flyTo({ ...MAP_PROVINCE, duration: 1200 });
	}

	// ── Catastro parcel layer (PMTiles) ─────────────────────────────────────

	// CARTO basemap building layer IDs (dark-matter style)
	const CARTO_BUILDING_LAYERS = ['building', 'building-top'];

	export function showCatastroLayer() {
		if (!map) return;
		// Retry if style not loaded yet
		if (!map.isStyleLoaded()) {
			map.once('idle', () => showCatastroLayer());
			return;
		}
		catastroActive = true;

		// Add catastro source
		if (!map.getSource('catastro')) {
			map.addSource('catastro', { type: 'vector', url: getTilesUrl('catastro'), maxzoom: 14 });
		}

		// Hide 3D buildings — add flat 2D fill BELOW catastro
		for (const layer of ALL_BUILDINGS_LAYERS) {
			if (map.getLayer(layer)) map.setLayoutProperty(layer, 'visibility', 'none');
		}
		if (!map.getLayer('buildings-flat') && map.getSource('buildings')) {
			map.addLayer({
				id: 'buildings-flat',
				type: 'fill',
				source: 'buildings',
				'source-layer': 'buildings',
				paint: {
					'fill-color': mapStore.getColorExpr() as any,
					'fill-opacity': 0.3
				}
			});
			map.on('click', 'buildings-flat', (e) => {
				if (lassoActive) return;
				if (catastroClickMode !== 'none') return;
				const redcode = e.features![0]?.properties?.redcode;
				if (!redcode) return;
				if (mapStore.hasRadio(redcode)) {
					container.dispatchEvent(new CustomEvent('radio-deselect', { bubbles: true, detail: { redcode } }));
				} else {
					const selected = [e.features![0].properties!];
					container.dispatchEvent(new CustomEvent('radio-select', {
						bubbles: true, detail: { redcode, selected, census: e.features![0].properties! }
					}));
				}
			});
			map.on('mouseenter', 'buildings-flat', () => { map.getCanvas().style.cursor = 'pointer'; });
			map.on('mouseleave', 'buildings-flat', () => { map.getCanvas().style.cursor = ''; });
		}

		// Fill layer — bright solid colors ON TOP of everything.
		// Highlight recently added parcels in amber and recently removed
		// parcels (ghost layer) in red against the urbano/rural base palette.
		if (!map.getLayer('catastro-fill')) {
			map.addLayer({
				id: 'catastro-fill',
				type: 'fill',
				source: 'catastro',
				'source-layer': 'catastro',
				minzoom: 9,
				paint: {
					'fill-color': [
						'case',
						['==', ['get', 'is_removed'], 1], '#dc2626',
						['==', ['get', 'is_new'], 1], '#fbbf24',
						[
							'match', ['get', 'tipo'],
							'urbano', '#22d3ee',
							'rural', '#4ade80',
							'#22d3ee'
						]
					],
					'fill-opacity': [
						'case',
						['==', ['get', 'is_removed'], 1], 0.45,
						0.75
					],
					'fill-outline-color': [
						'case',
						['==', ['get', 'is_removed'], 1], '#7f1d1d',
						['==', ['get', 'is_new'], 1], '#b45309',
						[
							'match', ['get', 'tipo'],
							'urbano', '#0e7490',
							'rural', '#15803d',
							'#0e7490'
						]
					]
				}
			});
		}

		// Line layer at high zoom for definition — on top of fill.
		// New parcels: darker amber outline. Removed parcels: thick dark red.
		if (!map.getLayer('catastro-line')) {
			map.addLayer({
				id: 'catastro-line',
				type: 'line',
				source: 'catastro',
				'source-layer': 'catastro',
				minzoom: 12,
				paint: {
					'line-color': [
						'case',
						['==', ['get', 'is_removed'], 1], '#fca5a5',
						['==', ['get', 'is_new'], 1], '#fde68a',
						'rgba(0,0,0,0.7)'
					],
					'line-width': [
						'interpolate', ['linear'], ['zoom'],
						12, ['case', ['==', ['get', 'is_removed'], 1], 1.2, ['==', ['get', 'is_new'], 1], 1.0, 0.8],
						14, ['case', ['==', ['get', 'is_removed'], 1], 2.0, ['==', ['get', 'is_new'], 1], 1.6, 1.2],
						17, ['case', ['==', ['get', 'is_removed'], 1], 3.0, ['==', ['get', 'is_new'], 1], 2.5, 2.0]
					],
					'line-opacity': 1.0
				}
			});
		}
		for (const layerId of CARTO_BUILDING_LAYERS) {
			if (map.getLayer(layerId)) {
				map.setLayoutProperty(layerId, 'visibility', 'none');
			}
		}
	}

	export function hideCatastroLayer() {
		if (!map || !map.isStyleLoaded() || !catastroActive) return;
		catastroActive = false;
		catastroClickMode = 'none';
		catastroClickBound = false; // layer being removed, handlers gone
		if (map.getLayer('catastro-fill')) map.removeLayer('catastro-fill');
		if (map.getLayer('catastro-line')) map.removeLayer('catastro-line');
		if (map.getLayer('buildings-flat')) map.removeLayer('buildings-flat');
		showBuildingsForActiveTerritory();
		for (const layerId of CARTO_BUILDING_LAYERS) {
			if (map.getLayer(layerId)) {
				map.setLayoutProperty(layerId, 'visibility', 'visible');
			}
		}
	}

	// ── Catastro flood choropleth (parcels colored by H3 flood risk) ────────

	// ── Unified catastro parcel click system ────────────────────────────
	// Single handler for ALL catastro-based analyses (flood, scores, radio)
	let catastroClickMode: 'none' | 'flood' | 'scores' = 'none';
	let catastroClickBound = false;

	function catastroUnifiedClickHandler(e: any) {
		if (lassoActive || catastroClickMode === 'none') return;
		const feat = e.features?.[0];
		if (!feat) return;
		const props = feat.properties;
		if (!props?.h3index) return;
		const detail = { h3index: props.h3index, tipo: props.tipo ?? 'urbano', area_m2: Number(props.area_m2) || 0 };
		const eventName = catastroClickMode === 'flood' ? 'catastro-flood-select' : 'catastro-scores-select';
		container.dispatchEvent(new CustomEvent(eventName, { bubbles: true, detail }));
	}

	function catastroMouseEnter() {
		if (catastroClickMode !== 'none' && !lassoActive) map.getCanvas().style.cursor = 'pointer';
	}
	function catastroMouseLeave() {
		if (catastroClickMode !== 'none' && !lassoActive) map.getCanvas().style.cursor = '';
	}

	function bindCatastroClick() {
		// Always clean up first, then bind once
		try {
			map.off('click', 'catastro-fill', catastroUnifiedClickHandler);
			map.off('mouseenter', 'catastro-fill', catastroMouseEnter);
			map.off('mouseleave', 'catastro-fill', catastroMouseLeave);
		} catch (_) { /* ok if layer doesn't exist */ }
		catastroClickBound = true;
		map.on('click', 'catastro-fill', catastroUnifiedClickHandler);
		map.on('mouseenter', 'catastro-fill', catastroMouseEnter);
		map.on('mouseleave', 'catastro-fill', catastroMouseLeave);
	}

	function unbindCatastroClick() {
		if (!catastroClickBound) return;
		catastroClickBound = false;
		try {
			map.off('click', 'catastro-fill', catastroUnifiedClickHandler);
			map.off('mouseenter', 'catastro-fill', catastroMouseEnter);
			map.off('mouseleave', 'catastro-fill', catastroMouseLeave);
		} catch (_) { /* layer may have been removed */ }
	}

	function applyCatastroChoropleth(colorExpr: any) {
		if (map.getLayer('catastro-fill')) {
			map.setPaintProperty('catastro-fill', 'fill-color', colorExpr);
			map.setPaintProperty('catastro-fill', 'fill-opacity',
				['interpolate', ['linear'], ['zoom'], 10, 0.25, 11, 0.35, 12, 0.55, 14, 0.7]);
		}
		if (map.getLayer('catastro-line')) {
			map.setPaintProperty('catastro-line', 'line-color', '#ffffff');
			map.setPaintProperty('catastro-line', 'line-opacity',
				['interpolate', ['linear'], ['zoom'], 10, 0.1, 11, 0.15, 12, 0.3, 14, 0.5]);
		}
	}

	function resetCatastroStyle() {
		if (map.getLayer('catastro-fill')) {
			map.setPaintProperty('catastro-fill', 'fill-color', [
				'match', ['get', 'tipo'], 'urbano', '#22d3ee', 'rural', '#4ade80', '#22d3ee'
			]);
			map.setPaintProperty('catastro-fill', 'fill-opacity', 0.95);
			map.setPaintProperty('catastro-fill', 'fill-outline-color', [
				'match', ['get', 'tipo'], 'urbano', '#0e7490', 'rural', '#15803d', '#0e7490'
			]);
		}
		if (map.getLayer('catastro-line')) {
			map.setPaintProperty('catastro-line', 'line-color', [
				'match', ['get', 'tipo'], 'urbano', '#0e7490', 'rural', '#15803d', '#0e7490'
			]);
			map.setPaintProperty('catastro-line', 'line-opacity',
				['interpolate', ['linear'], ['zoom'], 13, 0.6, 15, 0.9]);
		}
	}

	export function filterCatastroDept(deptCode: string | null) {
		if (!map) return;
		if (deptCode) {
			const filter = ['==', ['get', 'departamento'], deptCode];
			if (map.getLayer('catastro-fill')) map.setFilter('catastro-fill', filter);
			if (map.getLayer('catastro-line')) map.setFilter('catastro-line', filter);
		} else {
			if (map.getLayer('catastro-fill')) map.setFilter('catastro-fill', null);
			if (map.getLayer('catastro-line')) map.setFilter('catastro-line', null);
		}
	}

	export function setCatastroFloodChoropleth(h3ScoreMap: Map<string, number>) {
		if (!map) return;

		function apply() {
			if (!catastroActive) showCatastroLayer();
			catastroClickMode = 'flood';

			const matchExpr: any[] = ['match', ['get', 'h3index']];
			for (const [h3index, score] of h3ScoreMap) { matchExpr.push(h3index, score); }
			matchExpr.push(0);

			applyCatastroChoropleth([
				'interpolate', ['linear'], matchExpr,
				0, '#0d1b2a', 10, '#1b3a5f', 25, '#2a6f97',
				40, '#eab308', 60, '#f97316', 80, '#dc2626', 100, '#7f1d1d'
			]);
			bindCatastroClick();
		}

		if (map.isStyleLoaded() && !map.isMoving()) {
			apply();
		} else {
			map.once('idle', apply);
		}
	}

	export function clearCatastroFloodChoropleth() {
		if (!map || !map.isStyleLoaded()) return;
		catastroClickMode = 'none';
		resetCatastroStyle();
		unbindCatastroClick();
	}

	export function setFloodParcelHighlight(parcels: Array<{ h3index: string; color: string }>) {
		if (!map || !map.isStyleLoaded() || !map.getSource('catastro')) return;

		// Build filter: match any selected h3index
		if (parcels.length === 0) {
			if (map.getLayer('catastro-sel-fill')) map.removeLayer('catastro-sel-fill');
			if (map.getLayer('catastro-sel-line')) map.removeLayer('catastro-sel-line');
			return;
		}

		const h3Filter: any = ['in', ['get', 'h3index'], ['literal', parcels.map(p => p.h3index)]];

		// Color match: h3index → parcel color
		const colorMatch: any[] = ['match', ['get', 'h3index']];
		for (const p of parcels) { colorMatch.push(p.h3index, p.color); }
		colorMatch.push('#ffffff');

		if (!map.getLayer('catastro-sel-fill')) {
			map.addLayer({
				id: 'catastro-sel-fill',
				type: 'fill',
				source: 'catastro',
				'source-layer': 'catastro',
				minzoom: 11,
				paint: { 'fill-color': colorMatch, 'fill-opacity': 0.45 },
				filter: h3Filter
			});
		} else {
			map.setPaintProperty('catastro-sel-fill', 'fill-color', colorMatch);
			map.setFilter('catastro-sel-fill', h3Filter);
		}

		if (!map.getLayer('catastro-sel-line')) {
			map.addLayer({
				id: 'catastro-sel-line',
				type: 'line',
				source: 'catastro',
				'source-layer': 'catastro',
				minzoom: 11,
				paint: { 'line-color': colorMatch, 'line-width': 3, 'line-opacity': 0.9 },
				filter: h3Filter
			});
		} else {
			map.setPaintProperty('catastro-sel-line', 'line-color', colorMatch);
			map.setFilter('catastro-sel-line', h3Filter);
		}
	}

	export function clearFloodParcelHighlight() {
		if (!map || !map.isStyleLoaded()) return;
		if (map.getLayer('catastro-sel-fill')) map.removeLayer('catastro-sel-fill');
		if (map.getLayer('catastro-sel-line')) map.removeLayer('catastro-sel-line');
	}

	// ── Scores/Radio choropleth (catastro-based, reuses unified click) ──

	export function setCatastroScoresChoropleth(h3ScoreMap: Map<string, number>) {
		if (!map) return;

		function apply() {
			if (!catastroActive) showCatastroLayer();
			catastroClickMode = 'scores';

			const matchExpr: any[] = ['match', ['get', 'h3index']];
			for (const [h3index, score] of h3ScoreMap) { matchExpr.push(h3index, score); }
			matchExpr.push(0);

			applyCatastroChoropleth([
				'interpolate', ['linear'], matchExpr,
				0, '#1e293b', 15, '#334155', 30, '#4a7c59',
				50, '#22c55e', 70, '#86efac', 100, '#f0fdf4'
			]);
			bindCatastroClick();
		}

		if (map.isStyleLoaded() && !map.isMoving()) {
			apply();
		} else {
			map.once('idle', apply);
		}
	}

	export function clearCatastroScoresChoropleth() {
		if (!map || !map.isStyleLoaded()) return;
		catastroClickMode = 'none';
		resetCatastroStyle();
		unbindCatastroClick();
	}

	export function setScoresParcelHighlight(parcels: Array<{ h3index: string; color: string }>) {
		setFloodParcelHighlight(parcels);
	}

	// ── Analysis choropleth layers (radio-based, for non-catastro analyses) ──

	export function setAnalysisChoropleth(entries: { redcode: string; value: number }[], colorScale: 'price' | 'score' | 'diverging' | 'sequential' = 'price') {
		if (!map || !map.isStyleLoaded()) return;

		// All analysis types: use radios PMTiles (existing logic)
		if (!map.getLayer('analysis-fill')) {
			map.addLayer({
				id: 'analysis-fill',
				type: 'fill',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'fill-color': '#f59e0b', 'fill-opacity': 0.35 },
				filter: ['==', ['get', 'redcode'], '']
			});
		}
		if (!map.getLayer('analysis-line')) {
			map.addLayer({
				id: 'analysis-line',
				type: 'line',
				source: 'radios',
				'source-layer': 'radios',
				paint: { 'line-color': '#1e293b', 'line-width': 0.5, 'line-opacity': 0.6 },
				filter: ['==', ['get', 'redcode'], '']
			});
		}

		if (entries.length === 0) return;

		const values = entries.map(e => e.value);
		let minVal = Infinity, maxVal = -Infinity;
		for (const v of values) { if (v < minVal) minVal = v; if (v > maxVal) maxVal = v; }
		const range = maxVal - minVal || 1;

		const matchExpr: any[] = ['match', ['to-string', ['get', 'redcode']]];
		for (const entry of entries) {
			if (entry.value === 0) {
				matchExpr.push(String(entry.redcode), 'rgb(30,41,59)');
				continue;
			}
			const t = (entry.value - minVal) / range;
			let r: number, g: number, b: number;
			if (colorScale === 'sequential') {
				// Dark navy → bright cyan ramp for catastro density
				r = Math.round(13 + t * (20 - 13));
				g = Math.round(27 + t * (182 - 27));
				b = Math.round(42 + t * (212 - 42));
			} else if (colorScale === 'price') {
				r = Math.round(t < 0.5 ? 34 + t * 2 * (234 - 34) : 234 + (t - 0.5) * 2 * (239 - 234));
				g = Math.round(t < 0.5 ? 197 + t * 2 * (179 - 197) : 179 + (t - 0.5) * 2 * (68 - 179));
				b = Math.round(t < 0.5 ? 94 + t * 2 * (8 - 94) : 8 + (t - 0.5) * 2 * (68 - 8));
			} else {
				// Viridis: dark purple → teal → yellow
				r = Math.round(t < 0.5 ? 68 + t * 2 * (33 - 68) : 33 + (t - 0.5) * 2 * (253 - 33));
				g = Math.round(t < 0.5 ? 1 + t * 2 * (145 - 1) : 145 + (t - 0.5) * 2 * (231 - 145));
				b = Math.round(t < 0.5 ? 84 + t * 2 * (140 - 84) : 140 + (t - 0.5) * 2 * (37 - 140));
			}
			matchExpr.push(String(entry.redcode), `rgb(${r},${g},${b})`);
		}
		matchExpr.push('rgba(0,0,0,0)');

		const redcodes = entries.map(e => e.redcode);
		map.setFilter('analysis-fill', ['in', ['get', 'redcode'], ['literal', redcodes]]);
		map.setPaintProperty('analysis-fill', 'fill-color', matchExpr);
		map.setPaintProperty('analysis-fill', 'fill-opacity', 0.4);
		map.setFilter('analysis-line', ['in', ['get', 'redcode'], ['literal', redcodes]]);
	}

	export function clearAnalysisChoropleth() {
		if (!map || !map.isStyleLoaded()) return;
		// Remove catastro layers only if active
		if (catastroActive) hideCatastroLayer();
		// Clear radio analysis layers
		if (map.getLayer('analysis-fill')) {
			map.setFilter('analysis-fill', ['==', ['get', 'redcode'], '']);
		}
		if (map.getLayer('analysis-line')) {
			map.setFilter('analysis-line', ['==', ['get', 'redcode'], '']);
		}
		// Restore buildings (territory-aware)
		showBuildingsForActiveTerritory();
	}

	export function highlightSingleOpportunity(redcode: string, color: string) {
		if (!map) return;
		const matchFilter: any = ['==', ['get', 'redcode'], redcode];

		// Dedicated selection layers (pre-created on map load, always update)
		map.setPaintProperty('selected-fill', 'fill-color', color);
		map.setFilter('selected-fill', matchFilter);

		map.setPaintProperty('selected-line', 'line-color', color);
		map.setFilter('selected-line', matchFilter);

		// Building outlines (visible at high zoom)
		map.setPaintProperty('radio-highlight', 'line-color', color);
		map.setPaintProperty('radio-highlight', 'line-width', 5);
		map.setFilter('radio-highlight', matchFilter);
	}

	export function highlightComparisonPair(redcodeA: string, colorA: string, redcodeB: string, colorB: string) {
		setRadioHighlight([
			{ redcode: redcodeA, color: colorA },
			{ redcode: redcodeB, color: colorB }
		]);
		// Use thicker line for comparison visibility
		if (map?.getLayer('radio-highlight')) {
			map.setPaintProperty('radio-highlight', 'line-width', 5);
		}
	}

	// ── Hexagon H3 choropleth functions ──────────────────────────────────

	const CATEGORICAL_PALETTE = ['#1565c0', '#7e57c2', '#4db6ac', '#66bb6a', '#c0ca33', '#ffb74d', '#e65100', '#78909c'];
	// "Sin cobertura" — gris neutro. Debe ser distinguible del fondo (#374151) Y
	// no confundirse con NINGÚN extremo de las rampas de datos (el violeta bajo
	// #5b21b6 = "buen acceso" en capas censales se confundía con el azul-gris
	// anterior #4b6584). Gris neutro = convención cartográfica de "sin dato".
	const NODATA_COLOR = '#6b7280';

	function computeHexColor(value: number, colorScale: string, minVal: number, maxVal: number, range: number): string {
		if (typeof value !== 'number' || !Number.isFinite(value)) return NODATA_COLOR;
		// Legacy: value=0 era usado como proxy de nodata. Tras la refactorización,
		// setHexChoropleth maneja nodata explícitamente antes de llamar a esta función.
		// Este branch permanece como red de seguridad para callsites legacy.
		if (value === 0 && colorScale !== 'diverging' && colorScale !== 'categorical' && colorScale !== 'lisa') return NODATA_COLOR;
		if (colorScale === 'lisa') {
			const LISA: Record<number, string> = { 1: '#3b82f6', 2: '#60a5fa', 3: '#f97316', 4: '#ef4444' };
			return LISA[Math.round(value)] ?? 'rgb(55,65,81)';
		}
		if (colorScale === 'categorical') {
			const idx = Math.round(value) - 1;
			if (idx < 0) return 'rgb(55,65,81)';
			return CATEGORICAL_PALETTE[idx % CATEGORICAL_PALETTE.length];
		}
		let r: number, g: number, b: number;
		if (colorScale === 'diverging') {
			const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal)) || 1;
			const t = value / absMax;
			if (t < 0) {
				const s = -t;
				r = Math.round(163 + s * 76); g = Math.round(163 - s * 95); b = Math.round(163 - s * 95);
			} else {
				const s = t;
				r = Math.round(163 - s * 129); g = Math.round(163 + s * 34); b = Math.round(163 - s * 69);
			}
		} else {
			const t = Math.max(0, Math.min(1, (value - minVal) / range));
			if (colorScale === 'flood') {
				r = Math.round(t < 0.5 ? 59 + t * 2 * (234 - 59) : 234 + (t - 0.5) * 2 * (220 - 234));
				g = Math.round(t < 0.5 ? 130 + t * 2 * (179 - 130) : 179 + (t - 0.5) * 2 * (38 - 179));
				b = Math.round(t < 0.5 ? 246 + t * 2 * (8 - 246) : 8 + (t - 0.5) * 2 * (38 - 8));
			} else if (colorScale === 'green') {
				// Inverted vs the house "high value = brighter" convention: green saturation
				// reads as vegetation density, so high coverage/score = the dark saturated green
				// and low = pale (matches the land_use methodology text "más verde = más árboles").
				const tg = 1 - t;
				r = Math.round(tg < 0.5 ? 20 + tg * 2 * (22 - 20) : 22 + (tg - 0.5) * 2 * (187 - 22));
				g = Math.round(tg < 0.5 ? 83 + tg * 2 * (101 - 83) : 101 + (tg - 0.5) * 2 * (247 - 101));
				b = Math.round(tg < 0.5 ? 45 + tg * 2 * (52 - 45) : 52 + (tg - 0.5) * 2 * (208 - 52));
			} else if (colorScale === 'warm') {
				r = Math.round(t < 0.5 ? 120 + t * 2 * (245 - 120) : 245 + (t - 0.5) * 2 * (253 - 245));
				g = Math.round(t < 0.5 ? 53 + t * 2 * (158 - 53) : 158 + (t - 0.5) * 2 * (231 - 158));
				b = Math.round(t < 0.5 ? 15 + t * 2 * (11 - 15) : 11 + (t - 0.5) * 2 * (37 - 11));
			} else if (colorScale === 'night') {
				// Night activity (VIIRS): azul-noche casi negro (#0b1026) → ámbar de marca
				// (#f59e0b) → blanco cálido incandescente (#fff7d6). Piso oscuro + techo
				// blanco-hot ⇒ la alta actividad nocturna "salta" como luces de ciudad.
				r = Math.round(t < 0.5 ? 11 + t * 2 * (245 - 11) : 245 + (t - 0.5) * 2 * (255 - 245));
				g = Math.round(t < 0.5 ? 16 + t * 2 * (158 - 16) : 158 + (t - 0.5) * 2 * (247 - 158));
				b = Math.round(t < 0.5 ? 38 + t * 2 * (11 - 38) : 11 + (t - 0.5) * 2 * (214 - 11));
			} else {
				r = Math.round(t < 0.5 ? 91 + t * 2 * (33 - 91) : 33 + (t - 0.5) * 2 * (253 - 33));
				g = Math.round(t < 0.5 ? 33 + t * 2 * (145 - 33) : 145 + (t - 0.5) * 2 * (231 - 145));
				b = Math.round(t < 0.5 ? 182 + t * 2 * (140 - 182) : 140 + (t - 0.5) * 2 * (37 - 140));
			}
		}
		return `rgb(${r},${g},${b})`;
	}

	// Track last state to avoid redundant setPaintProperty calls
	let _hexLayerInitialized = false;

	export function setHexChoropleth(entries: { h3index: string; value: number | null; properties?: Record<string, number>; boundary?: number[][]; nodata?: boolean }[], colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' | 'lisa' = 'flood', domain?: [number, number]) {
		if (!map) return;
		const src = map.getSource('hexagons') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;

		if (entries.length === 0) {
			src.setData({ type: 'FeatureCollection', features: [] });
			map.setPaintProperty('hex-fill', 'fill-opacity', 0);
			map.setPaintProperty('hex-line', 'line-opacity', 0);
			return;
		}

		const isNodata = (e: { value: number | null; nodata?: boolean }) =>
			e.nodata === true || e.value === null || typeof e.value !== 'number' || !Number.isFinite(e.value);

		let minVal: number, maxVal: number;
		if (domain && colorScale !== 'diverging' && colorScale !== 'categorical') {
			// Use provincial percentile bounds (P2/P98) for consistent cross-department coloring
			[minVal, maxVal] = domain;
		} else {
			// Fallback: local min/max from entries (skip nodata)
			minVal = Infinity; maxVal = -Infinity;
			for (const e of entries) {
				if (isNodata(e)) continue;
				const v = e.value as number;
				if (v < minVal) minVal = v;
				if (v > maxVal) maxVal = v;
			}
			if (!Number.isFinite(minVal)) { minVal = 0; maxVal = 1; }
		}
		const range = maxVal - minVal || 1;

		const getColor = (value: number) => computeHexColor(value, colorScale, minVal, maxVal, range);

		// Pre-allocate to avoid dynamic resizing (measurable on 30K+ hex sets)
		const features = new Array(entries.length);
		let fi = 0;
		for (const entry of entries) {
			if (!entry.boundary) continue;
			if (isNodata(entry)) {
				features[fi++] = {
					type: 'Feature',
					properties: {
						h3index: entry.h3index,
						value: null,
						color: NODATA_COLOR,
						nodata: true,
						type_label: entry.properties?.type_label
					},
					geometry: { type: 'Polygon', coordinates: [entry.boundary] }
				};
				continue;
			}
			features[fi++] = {
				type: 'Feature',
				properties: {
					h3index: entry.h3index,
					value: entry.value,
					color: getColor(entry.value as number),
					type_label: entry.properties?.type_label
				},
				geometry: { type: 'Polygon', coordinates: [entry.boundary] }
			};
		}
		if (fi < features.length) features.length = fi;

		src.setData({ type: 'FeatureCollection', features });
		// Static properties (color expression, line style) only need to be set once.
		// Opacity is always restored because clearHexChoropleth() sets it to 0.
		if (!_hexLayerInitialized) {
			map.setPaintProperty('hex-fill', 'fill-color', ['get', 'color']);
			map.setPaintProperty('hex-line', 'line-color', '#374151');
			map.setPaintProperty('hex-line', 'line-width', 0.5);
			_hexLayerInitialized = true;
		}
		map.setPaintProperty('hex-fill', 'fill-opacity', mapStore.hexOpacity);
		map.setPaintProperty('hex-line', 'line-opacity', mapStore.hexOpacity * 0.32);

		const bgSrc = map.getSource('territory-bg') as maplibregl.GeoJSONSource | undefined;
		if (bgSrc) {
			const bgData = activeTerritoryId === 'corrientes' ? corrientesBoundary
			             : activeTerritoryId === 'itapua_py'  ? itapuaBoundary
			             : activeTerritoryId === 'alto_parana_py' ? altoParanaBoundary
			             : misionesBoundary;
			bgSrc.setData(bgData as any);
		}
	}

	export function clearHexChoropleth() {
		if (!map) return;
		const src = map.getSource('hexagons') as maplibregl.GeoJSONSource | undefined;
		if (src) src.setData({ type: 'FeatureCollection', features: [] });
		if (map.getLayer('hex-fill')) map.setPaintProperty('hex-fill', 'fill-opacity', 0);
		if (map.getLayer('hex-line')) map.setPaintProperty('hex-line', 'line-opacity', 0);
		if (map.getLayer('hex-selected')) map.setFilter('hex-selected', ['==', ['get', 'h3index'], '']);
		if (map.getLayer('compare-hex-selected')) map.setFilter('compare-hex-selected', ['==', ['get', 'h3index'], '']);
		const bgSrc = map.getSource('territory-bg') as maplibregl.GeoJSONSource | undefined;
		if (bgSrc) bgSrc.setData({ type: 'FeatureCollection', features: [] });
	}

	export function setCompareHexChoropleth(entries: { h3index: string; value: number | null; properties?: Record<string, number>; boundary?: number[][]; nodata?: boolean }[], colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = 'sequential', domain?: [number, number]) {
		if (!map) return;
		const src = map.getSource('compare-hexagons') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;

		if (entries.length === 0) {
			src.setData({ type: 'FeatureCollection', features: [] });
			map.setPaintProperty('compare-hex-fill', 'fill-opacity', 0);
			map.setPaintProperty('compare-hex-line', 'line-opacity', 0);
			return;
		}

		const isNodata = (e: { value: number | null; nodata?: boolean }) =>
			e.nodata === true || e.value === null || typeof e.value !== 'number' || !Number.isFinite(e.value);

		let minVal: number, maxVal: number;
		if (domain && colorScale !== 'diverging' && colorScale !== 'categorical') {
			[minVal, maxVal] = domain;
		} else {
			minVal = Infinity; maxVal = -Infinity;
			for (const e of entries) {
				if (isNodata(e)) continue;
				const v = e.value as number;
				if (v < minVal) minVal = v;
				if (v > maxVal) maxVal = v;
			}
			if (!Number.isFinite(minVal)) { minVal = 0; maxVal = 1; }
		}
		const range = maxVal - minVal || 1;
		const getColor = (v: number) => computeHexColor(v, colorScale, minVal, maxVal, range);

		const features = new Array(entries.length);
		let fi = 0;
		for (const entry of entries) {
			if (!entry.boundary) continue;
			if (isNodata(entry)) {
				features[fi++] = {
					type: 'Feature',
					properties: { h3index: entry.h3index, value: null, color: NODATA_COLOR, nodata: true, type_label: entry.properties?.type_label },
					geometry: { type: 'Polygon', coordinates: [entry.boundary] }
				};
				continue;
			}
			features[fi++] = {
				type: 'Feature',
				properties: { h3index: entry.h3index, value: entry.value, color: getColor(entry.value as number), type_label: entry.properties?.type_label },
				geometry: { type: 'Polygon', coordinates: [entry.boundary] }
			};
		}
		if (fi < features.length) features.length = fi;

		src.setData({ type: 'FeatureCollection', features });
		map.setPaintProperty('compare-hex-fill', 'fill-color', ['get', 'color']);
		map.setPaintProperty('compare-hex-fill', 'fill-opacity', mapStore.hexOpacity);
		map.setPaintProperty('compare-hex-line', 'line-color', '#374151');
		map.setPaintProperty('compare-hex-line', 'line-width', 0.5);
		map.setPaintProperty('compare-hex-line', 'line-opacity', mapStore.hexOpacity * 0.32);
	}

	export function clearCompareHexChoropleth() {
		if (!map) return;
		const src = map.getSource('compare-hexagons') as maplibregl.GeoJSONSource | undefined;
		if (src) src.setData({ type: 'FeatureCollection', features: [] });
		if (map.getLayer('compare-hex-fill')) map.setPaintProperty('compare-hex-fill', 'fill-opacity', 0);
		if (map.getLayer('compare-hex-line')) map.setPaintProperty('compare-hex-line', 'line-opacity', 0);
		if (map.getLayer('compare-hex-selected')) map.setFilter('compare-hex-selected', ['==', ['get', 'h3index'], '']);
	}

	export function setRegionalHexChoropleth(entries: { h3index: string; value: number | null; properties?: Record<string, number>; boundary?: number[][]; nodata?: boolean }[], colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = 'sequential', domain?: [number, number]) {
		if (!map) return;
		const src = map.getSource('regional-hexagons') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;

		if (entries.length === 0) {
			src.setData({ type: 'FeatureCollection', features: [] });
			map.setPaintProperty('regional-hex-fill', 'fill-opacity', 0);
			map.setPaintProperty('regional-hex-line', 'line-opacity', 0);
			return;
		}

		const isNodata = (e: { value: number | null; nodata?: boolean }) =>
			e.nodata === true || e.value === null || typeof e.value !== 'number' || !Number.isFinite(e.value);

		let minVal: number, maxVal: number;
		if (domain && colorScale !== 'diverging' && colorScale !== 'categorical') {
			[minVal, maxVal] = domain;
		} else {
			minVal = Infinity; maxVal = -Infinity;
			for (const e of entries) {
				if (isNodata(e)) continue;
				const v = e.value as number;
				if (v < minVal) minVal = v;
				if (v > maxVal) maxVal = v;
			}
			if (!Number.isFinite(minVal)) { minVal = 0; maxVal = 1; }
		}
		const range = maxVal - minVal || 1;
		const getColor = (v: number) => computeHexColor(v, colorScale, minVal, maxVal, range);

		const features: any[] = [];
		for (const entry of entries) {
			if (!entry.boundary) continue;
			if (isNodata(entry)) {
				features.push({
					type: 'Feature',
					properties: { h3index: entry.h3index, value: null, color: NODATA_COLOR, nodata: true, type_label: entry.properties?.type_label },
					geometry: { type: 'Polygon', coordinates: [entry.boundary] }
				});
				continue;
			}
			features.push({
				type: 'Feature',
				properties: { h3index: entry.h3index, value: entry.value, color: getColor(entry.value as number), type_label: entry.properties?.type_label },
				geometry: { type: 'Polygon', coordinates: [entry.boundary] }
			});
		}

		src.setData({ type: 'FeatureCollection', features });
		map.setPaintProperty('regional-hex-fill', 'fill-color', ['get', 'color']);
		map.setPaintProperty('regional-hex-fill', 'fill-opacity', mapStore.hexOpacity);
		map.setPaintProperty('regional-hex-line', 'line-color', '#374151');
		map.setPaintProperty('regional-hex-line', 'line-width', 0.5);
		map.setPaintProperty('regional-hex-line', 'line-opacity', mapStore.hexOpacity * 0.32);
	}

	export function clearRegionalHexChoropleth() {
		if (!map) return;
		const src = map.getSource('regional-hexagons') as maplibregl.GeoJSONSource | undefined;
		if (src) src.setData({ type: 'FeatureCollection', features: [] });
		if (map.getLayer('regional-hex-fill')) map.setPaintProperty('regional-hex-fill', 'fill-opacity', 0);
		if (map.getLayer('regional-hex-line')) map.setPaintProperty('regional-hex-line', 'line-opacity', 0);
	}

	// Apply the user-controlled hex opacity (bottom-left slider) to whichever hex slots are
	// currently painted, without forcing a full re-render. Layers at fill-opacity 0 (cleared
	// / not in use) stay hidden — we only touch slots that are already visible.
	export function applyHexOpacity() {
		if (!map) return;
		const o = mapStore.hexOpacity;
		for (const [fill, line] of [
			['hex-fill', 'hex-line'],
			['compare-hex-fill', 'compare-hex-line'],
			['regional-hex-fill', 'regional-hex-line'],
		]) {
			if (!map.getLayer(fill)) continue;
			const cur = map.getPaintProperty(fill, 'fill-opacity');
			if (typeof cur === 'number' && cur === 0) continue; // slot not in use → leave hidden
			map.setPaintProperty(fill, 'fill-opacity', o);
			if (map.getLayer(line)) map.setPaintProperty(line, 'line-opacity', o * 0.32);
		}
	}

	// Sentinel-2 satellite underlay (EOX cloudless, served via /api/satellite proxy). Added
	// lazily below the hex/label layers so it reads as a reference basemap. Additive — never
	// setStyle (which would wipe the ~40 mounted layers).
	export function setSatellite(active: boolean) {
		if (!map) return;
		if (active && !map.getSource('satellite')) {
			map.addSource('satellite', {
				type: 'raster',
				tiles: ['/api/satellite/{z}/{x}/{y}.jpg'],
				tileSize: 256,
				maxzoom: 16,
				attribution: 'Sentinel-2 cloudless 2024 © EOX — modified Copernicus Sentinel data 2024'
			});
			// Insert just below hex-fill (above the gray "missing hex" territory-bg, so the
			// imagery shows cleanly), and below the label layers → order = dark base ·
			// territory-bg · satellite · hex · labels · buildings-3D.
			const beforeId = map.getLayer('hex-fill') ? 'hex-fill' : firstSymbolId;
			map.addLayer({
				id: 'satellite-layer',
				type: 'raster',
				source: 'satellite',
				// EOX s2cloudless renders flat/hazy; push contrast + saturation and clip the
				// highlights so it has more punch. (Resolution stays 10 m — no real sharpen.)
				paint: {
					'raster-opacity': 1,
					'raster-contrast': 0.28,
					'raster-saturation': 0.35,
					'raster-brightness-max': 0.94,
					'raster-resampling': 'nearest'
				}
			}, beforeId);
		}
		if (map.getLayer('satellite-layer')) {
			map.setLayoutProperty('satellite-layer', 'visibility', active ? 'visible' : 'none');
		}
	}

	export function highlightHexagon(h3index: string) {
		if (!map || !map.getLayer('hex-selected')) return;
		if (!h3index) {
			map.setFilter('hex-selected', ['==', ['get', 'h3index'], '']);
			return;
		}
		map.setFilter('hex-selected', ['==', ['get', 'h3index'], h3index]);
	}

	export function highlightHexagons(
		hexes: { h3index: string; color: string }[],
		compareHexes?: { h3index: string; color: string }[]
	) {
		if (!map || !map.getLayer('hex-selected')) return;
		if (hexes.length === 0) {
			map.setFilter('hex-selected', ['==', ['get', 'h3index'], '']);
		} else {
			const ids = hexes.map(h => h.h3index);
			const matchExpr: any[] = ['match', ['get', 'h3index']];
			for (const h of hexes) matchExpr.push(h.h3index, h.color);
			matchExpr.push('#ffffff');
			map.setPaintProperty('hex-selected', 'line-color', matchExpr);
			map.setFilter('hex-selected', ['in', ['get', 'h3index'], ['literal', ids]]);
		}

		if (!map.getLayer('compare-hex-selected')) return;
		if (!compareHexes || compareHexes.length === 0) {
			map.setFilter('compare-hex-selected', ['==', ['get', 'h3index'], '']);
			return;
		}
		const cIds = compareHexes.map(h => h.h3index);
		const cMatch: any[] = ['match', ['get', 'h3index']];
		for (const h of compareHexes) cMatch.push(h.h3index, h.color);
		cMatch.push('#f59e0b');
		map.setPaintProperty('compare-hex-selected', 'line-color', cMatch);
		map.setFilter('compare-hex-selected', ['in', ['get', 'h3index'], ['literal', cIds]]);
	}

	// ── Hex zone highlight functions ──────────────────────────────────────

	export function setHexZoneHighlight(zones: { h3indices: string[]; color: string }[], boundaryCache?: Map<string, number[][]>) {
		if (!map) return;
		const src = map.getSource('hex-zones') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;

		if (zones.length === 0) {
			src.setData({ type: 'FeatureCollection', features: [] });
			return;
		}

		const features: any[] = [];
		for (const zone of zones) {
			for (const h3index of zone.h3indices) {
				const cached = boundaryCache?.get(h3index);
				if (!cached) continue;
				features.push({
					type: 'Feature',
					properties: { h3index, color: zone.color },
					geometry: { type: 'Polygon', coordinates: [cached] }
				});
			}
		}
		src.setData({ type: 'FeatureCollection', features });
	}

	export function clearHexZoneHighlight() {
		if (!map) return;
		const src = map.getSource('hex-zones') as maplibregl.GeoJSONSource | undefined;
		if (src) src.setData({ type: 'FeatureCollection', features: [] });
	}

	// Census-panel chart brush → highlight matching radios on the PMTiles layer.
	export function clearRadioBrushHighlight() {
		if (!map) return;
		const e: any = ['==', ['get', 'redcode'], ''];
		if (map.getLayer('radio-brush-fill')) map.setFilter('radio-brush-fill', e);
		if (map.getLayer('radio-brush-line')) map.setFilter('radio-brush-line', e);
	}
	export function setRadioBrushHighlight(redcodes: string[]) {
		if (!map) return;
		if (redcodes.length === 0) { clearRadioBrushHighlight(); return; }
		const f: any = ['in', ['get', 'redcode'], ['literal', redcodes]];
		if (map.getLayer('radio-brush-fill')) map.setFilter('radio-brush-fill', f);
		if (map.getLayer('radio-brush-line')) map.setFilter('radio-brush-line', f);
	}

	// ── Lasso / Zone functions ────────────────────────────────────────────

	export function setLassoMode(active: boolean) {
		lassoActive = active;
		if (!map) return;
		map.getCanvas().style.cursor = active ? 'crosshair' : '';
		if (active) {
			map.dragPan.disable();
		} else {
			map.dragPan.enable();
		}
	}

	export function updateLassoDraw(polygon: [number, number][]) {
		if (!map) return;
		const src = map.getSource('lasso-draw') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		const coords = polygon.length >= 3
			? [[...polygon, polygon[0]]]
			: polygon.length >= 2
				? [polygon]  // just show as open line during draw
				: [[]];
		src.setData({
			type: 'Feature',
			geometry: { type: 'Polygon', coordinates: coords },
			properties: {}
		});
	}

	// Rough centroid of a (Multi)Polygon feature geometry — good enough for
	// polygon containment of building footprints.
	function _featureCentroid(geom: any): [number, number] | null {
		if (!geom) return null;
		let ring: any[] | null = null;
		if (geom.type === 'Polygon') ring = geom.coordinates?.[0];
		else if (geom.type === 'MultiPolygon') ring = geom.coordinates?.[0]?.[0];
		if (!ring || ring.length === 0) return null;
		let sx = 0, sy = 0;
		for (const [x, y] of ring) { sx += x; sy += y; }
		return [sx / ring.length, sy / ring.length];
	}

	// Lasso over the buildings canvas where there are no census radios (PY):
	// behave like the building click but multi — return the distinct districts
	// whose GBA buildings fall inside the drawn polygon. The caller then
	// selects those districts (DGEEC profile), never a fabricated zone.
	// Viewport-limited (same as the building click) — you select what you see.
	export function queryDistrictsInPolygon(
		polygon: [number, number][]
	): Array<{ distrito: string; territory: string; personas: number }> {
		if (!map) return [];
		const LAYER_TERR: Record<string, string> = {
			'itapua-buildings-3d': 'itapua_py',
			'alto_parana-buildings-3d': 'alto_parana_py',
		};
		const layers = Object.keys(LAYER_TERR).filter(l => map.getLayer(l));
		if (layers.length === 0) return [];
		const cv = map.getCanvas();
		const feats = map.queryRenderedFeatures([[0, 0], [cv.width, cv.height]], { layers });
		const seen = new Set<string>();
		const out: Array<{ distrito: string; territory: string; personas: number }> = [];
		for (const f of feats) {
			const p: any = f.properties || {};
			const distrito = p.distrito;
			if (!distrito) continue;
			const territory = LAYER_TERR[f.layer?.id ?? ''] ?? 'itapua_py';
			const key = `${territory}|${distrito}`;
			if (seen.has(key)) continue;
			const c = _featureCentroid(f.geometry);
			if (!c || !pointInPolygon(c, polygon)) continue;
			seen.add(key);
			// district DGEEC population (same field the building click passes)
			const personas = Number(p.distrito_pop) || Number(p.est_personas) || 0;
			out.push({ distrito, territory, personas });
		}
		return out;
	}

	export function queryBrSetoresInPolygon(polygon: [number, number][]): string[] {
		if (!map) return [];
		const brLayers = ['parana_br-buildings-3d', 'santa_catarina_br-buildings-3d', 'rio_grande_sul_br-buildings-3d']
			.filter(l => map.getLayer(l));
		if (brLayers.length === 0) return [];
		const cv = map.getCanvas();
		const feats = map.queryRenderedFeatures([[0, 0], [cv.width, cv.height]], { layers: brLayers });
		const seen = new Set<string>();
		for (const f of feats) {
			const rc = f.properties?.redcode;
			if (!rc) continue;
			const c = _featureCentroid(f.geometry);
			if (!c || !pointInPolygon(c, polygon)) continue;
			seen.add(String(rc));
		}
		return [...seen];
	}

	export function clearLassoDraw() {
		if (!map) return;
		const src = map.getSource('lasso-draw') as maplibregl.GeoJSONSource | undefined;
		if (!src) return;
		src.setData({
			type: 'Feature',
			geometry: { type: 'Polygon', coordinates: [[]] },
			properties: {}
		});
	}

	export function setZoneHighlight(zones: { redcodes: string[]; color: string }[]) {
		if (!map) return;
		const emptyFilter: any = ['==', ['get', 'redcode'], ''];

		if (zones.length === 0) {
			for (const l of ['zone-fill', 'zone-line', 'zone-buildings',
				'zone-buildings-corrientes', 'zone-buildings-chaco', 'zone-buildings-formosa',
				'zone-buildings-parana_br', 'zone-buildings-santa_catarina_br', 'zone-buildings-rio_grande_sul_br']) {
				if (map.getLayer(l)) map.setFilter(l, emptyFilter);
			}
			return;
		}

		// Collect all redcodes and build match expression for colors
		const allRedcodes: string[] = [];
		const matchExpr: any[] = ['match', ['get', 'redcode']];
		for (const zone of zones) {
			for (const rc of zone.redcodes) {
				allRedcodes.push(rc);
				matchExpr.push(rc, zone.color);
			}
		}
		matchExpr.push('rgba(0,0,0,0)'); // fallback

		const filter: any = ['in', ['get', 'redcode'], ['literal', allRedcodes]];

		if (map.getLayer('zone-fill')) {
			map.setPaintProperty('zone-fill', 'fill-color', matchExpr);
			map.setFilter('zone-fill', filter);
		}
		if (map.getLayer('zone-line')) {
			map.setPaintProperty('zone-line', 'line-color', matchExpr);
			map.setFilter('zone-line', filter);
		}
		// Building outlines: highlight both territory layers (regional mode shows mixed zones)
		if (map.getLayer('zone-buildings')) {
			map.setPaintProperty('zone-buildings', 'line-color', matchExpr);
			map.setFilter('zone-buildings', filter);
		}
		if (map.getLayer('zone-buildings-corrientes')) {
			map.setPaintProperty('zone-buildings-corrientes', 'line-color', matchExpr);
			map.setFilter('zone-buildings-corrientes', filter);
		}
		if (map.getLayer('zone-buildings-chaco')) {
			map.setPaintProperty('zone-buildings-chaco', 'line-color', matchExpr);
			map.setFilter('zone-buildings-chaco', filter);
		}
		if (map.getLayer('zone-buildings-formosa')) {
			map.setPaintProperty('zone-buildings-formosa', 'line-color', matchExpr);
			map.setFilter('zone-buildings-formosa', filter);
		}
		for (const tid of ['parana_br', 'santa_catarina_br', 'rio_grande_sul_br']) {
			const l = `zone-buildings-${tid}`;
			if (map.getLayer(l)) {
				map.setPaintProperty(l, 'line-color', matchExpr);
				map.setFilter(l, filter);
			}
		}
	}

	export function clearZoneHighlight() {
		if (!map) return;
		const emptyFilter: any = ['==', ['get', 'redcode'], ''];
		if (map.getLayer('zone-fill')) map.setFilter('zone-fill', emptyFilter);
		if (map.getLayer('zone-line')) map.setFilter('zone-line', emptyFilter);
		if (map.getLayer('zone-buildings')) map.setFilter('zone-buildings', emptyFilter);
		if (map.getLayer('zone-buildings-corrientes')) map.setFilter('zone-buildings-corrientes', emptyFilter);
		if (map.getLayer('zone-buildings-chaco')) map.setFilter('zone-buildings-chaco', emptyFilter);
		if (map.getLayer('zone-buildings-formosa')) map.setFilter('zone-buildings-formosa', emptyFilter);
	}

	export function getLassoActive(): boolean {
		return lassoActive;
	}

	export function getMap(): maplibregl.Map | null {
		return map ?? null;
	}
</script>

<div bind:this={container} style="width:100%;height:100%;"></div>

<style>
	/* Bottom-right control stack: force the zoom ABOVE the credit line, and pin the
	   attribution flush to the corner. MapLibre injects this DOM, so :global() is required. */
	:global(.maplibregl-ctrl-bottom-right) {
		display: flex;
		flex-direction: column;
		align-items: flex-end;
	}
	:global(.maplibregl-ctrl-bottom-right .maplibregl-ctrl-group) {
		order: 1; /* zoom on top */
		margin: 0 8px 6px 0;
	}
	:global(.maplibregl-ctrl-bottom-right .maplibregl-ctrl-attrib) {
		order: 2; /* credit at the very bottom-right corner */
		background: transparent;
		margin: 0;
		padding: 0 6px 3px 0;
		max-width: 360px;
		text-align: right;
	}
	:global(.maplibregl-ctrl-attrib-inner),
	:global(.maplibregl-ctrl-attrib),
	:global(.maplibregl-ctrl-attrib a),
	:global(.maplibregl-ctrl-attrib-inner a) {
		color: #fff !important;
		font-size: 9px;
		line-height: 1.25;
		text-decoration: none;
		text-shadow: 0 1px 2px rgba(0, 0, 0, 0.9);
	}
</style>
