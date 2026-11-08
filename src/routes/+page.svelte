<script lang="ts">
	import { onMount, untrack, tick } from 'svelte';
	import MapComponent from '$lib/components/Map.svelte';
	import MapLegend from '$lib/components/MapLegend.svelte';
	import { SELLO_HEX } from '$lib/utils/sello';
	import { encodeZones, decodeZones, encodeZoneNames, decodeZoneNames } from '$lib/utils/zoneCodec';
	import Controls from '$lib/components/Controls.svelte';
	import Sidebar from '$lib/components/Sidebar.svelte';
	import LensSelector from '$lib/components/LensSelector.svelte';
	import { MapStore } from '$lib/stores/map.svelte';
	import { LensStore } from '$lib/stores/lens.svelte';
	import { LassoStore } from '$lib/stores/lasso.svelte';
	import { HexStore } from '$lib/stores/hex.svelte';
	import { radioUnit, isRadioSupport, codprovOf, radioSelectionOn } from '$lib/stores/radioUnit.svelte';
	import { TerritoryStore } from '$lib/stores/territory.svelte';
	import { initDuckDB, query, isReady, getInitError } from '$lib/stores/duckdb';
	import { cellToLatLng, polygonToCells } from 'h3-js';
	import { isInsideMisiones } from '$lib/utils/misiones-pip';
	import { isInsideItapua } from '$lib/utils/itapua-pip';
	import { isInsideCorrientes } from '$lib/utils/corrientes-pip';
	import { isInsideAltoParana } from '$lib/utils/alto_parana-pip';
	import { isInsideChaco } from '$lib/utils/chaco-pip';
	import { isInsideFormosa } from '$lib/utils/formosa-pip';
	import { isInsideParanaBr } from '$lib/utils/parana_br-pip';
	import { isInsideSantaCatarinaBr } from '$lib/utils/santa_catarina_br-pip';
	import { isInsideRioGrandeSulBr } from '$lib/utils/rio_grande_sul_br-pip';
	import { isInsideConcepcion } from '$lib/utils/concepcion-pip';
	import { isInsideSanPedro } from '$lib/utils/san_pedro-pip';
	import { isInsideCordillera } from '$lib/utils/cordillera-pip';
	import { isInsideGuaira } from '$lib/utils/guaira-pip';
	import { isInsideCaaguazu } from '$lib/utils/caaguazu-pip';
	import { isInsideCaazapa } from '$lib/utils/caazapa-pip';
	import { isInsideMisionesPy } from '$lib/utils/misiones_py-pip';
	import { isInsideParaguari } from '$lib/utils/paraguari-pip';
	import { isInsideCentral } from '$lib/utils/central-pip';
	import { isInsideNeembucu } from '$lib/utils/neembucu-pip';
	import { isInsideAmambay } from '$lib/utils/amambay-pip';
	import { isInsideCanindeyu } from '$lib/utils/canindeyu-pip';
	import { isInsidePresidenteHayes } from '$lib/utils/presidente_hayes-pip';
	import { isInsideBoqueron } from '$lib/utils/boqueron-pip';
	import { isInsideAltoParaguay } from '$lib/utils/alto_paraguay-pip';
	import { findDeptFeature, ensureBrBoundaries, isArDeptTerritory, normDeptName } from '$lib/utils/deptBoundaries';
	import { loadDeptSummary } from '$lib/utils/deptSummaries';
	import DeptPickerPanel from '$lib/components/DeptPickerPanel.svelte';
	import { PARQUETS, MAP_INIT, HEX_LAYER_REGISTRY, TERRITORY_REGISTRY, ANALYSIS_REGISTRY, getDefaultTerritory, getAnalysisById, getAnalysesForLens, getHexPrimaryUnit, type AnalysisConfig, type LensId, type CountryId } from '$lib/config';
	import { i18n, type Locale } from '$lib/stores/i18n.svelte';
	import { terms } from '$lib/stores/terms.svelte';
	import { page } from '$app/stores';
	import { themeStore, type Fondo } from '$lib/stores/theme.svelte';

	// Hex opacity defaults per basemap: in Satélite the slider starts just under its
	// middle (range 0.3–0.9) so the imagery reads under the hexes; on paper (Claro)
	// near opaque, or every ramp washes out (sello.ts: Claro ramps are validated as
	// drawn at 0.88). A value the user set by hand is kept when switching (±0.03: the
	// slider snaps 0.78 to its 0.05 grid).
	const HEX_OPACITY: Record<Fondo, number> = { claro: 0.88, oscuro: 0.78, satelite: 0.55 };
	const mapStore = new MapStore();
	mapStore.hexOpacity = HEX_OPACITY[themeStore.fondo];
	const FONDOS: Fondo[] = ['claro', 'oscuro', 'satelite'];
	function setFondo(f: Fondo) {
		if (f === themeStore.fondo) return;
		const toSat = f === 'satelite', fromSat = themeStore.fondo === 'satelite';
		// A value the user set by hand is kept; a default follows the basemap.
		const byHand = Math.abs(mapStore.hexOpacity - HEX_OPACITY[themeStore.fondo]) >= 0.03;
		if (!byHand) mapStore.hexOpacity = HEX_OPACITY[f];
		themeStore.set(f);
		if (toSat !== fromSat) mapComponent?.setSatellite(toSat);
		else if (!byHand) mapComponent?.applyHexOpacity();
	}
	const lensStore = new LensStore();
	const lassoStore = new LassoStore();
	const hexStore = new HexStore();
	const territoryStore = new TerritoryStore();

	let mapComponent: ReturnType<typeof MapComponent>;
	let mapContainer: HTMLDivElement;
	let duckdbFailed = $state(false);
	let itapuaHovering = $state(false);
	// ── URL state: read params on mount, write on change ──
	// Captured at component init: the $effect below flushes before onMount's
	// restore and would otherwise wipe incoming deep-link params via replaceState.
	const initialSearch = typeof window !== 'undefined' ? window.location.search : '';
	function updateUrlState() {
		const params = new URLSearchParams();
		if (territoryStore.activeTerritory.id !== 'misiones') params.set('t', territoryStore.activeTerritory.id);
		if (lensStore.activeLens) params.set('lens', lensStore.activeLens);
		if (lensStore.activeAnalysis) params.set('a', lensStore.activeAnalysis.id);
		// censo_temporal opens its department through its list (deptFilterName), not selectedDpto.
		const dept = hexStore.selectedDpto ?? (hexStore.activeLayer?.perDepartment ? null : hexStore.deptFilterName);
		if (dept) params.set('dept', dept);
		if (hexStore.temporalMode !== 'current') params.set('tm', hexStore.temporalMode);
		if (hexStore.scaleMode !== 'provincial') params.set('escala', hexStore.scaleMode);
		if (territoryStore.countryFilter && territoryStore.countryFilter !== 'ar') params.set('cf', territoryStore.countryFilter);
		// Lasso zones (utils/zoneCodec.ts): a shared link brings them back. Last, so a long
		// outline never pushes the layer and department out of a truncated link.
		const zs = hexStore.urlZones;
		if (zs.length) params.set('z', encodeZones(zs.map(z => ({ id: z.id, n: z.h3indices.length, outline: z.outline! }))));
		// Names after the zones: a cut link loses the names before the places.
		const zn = encodeZoneNames(zs);
		if (zn) params.set('zn', zn);
		const qs = params.toString();
		window.history.replaceState({}, '', qs ? `?${qs}` : window.location.pathname);
	}

	$effect(() => {
		const _cf = territoryStore.countryFilter;
		const _t = territoryStore.activeTerritory.id;
		const _lens = lensStore.activeLens;
		const _analysis = lensStore.activeAnalysis;
		const _dept = hexStore.selectedDpto;
		const _deptF = hexStore.deptFilterName;
		const _tm = hexStore.temporalMode;
		const _escala = hexStore.scaleMode;
		const _zones = hexStore.urlZones;
		if (typeof window !== 'undefined') updateUrlState();
	});

	const REGIONAL_NEA_BBOX: [number, number, number, number] = [-59.8, -30.8, -53.6, -25.4];
	// Start view and Home button: the 4 NEA provinces, the strong outline (7-nov-2026). The
	// regional box above (Misiones + Corrientes + the Paraná border) still frames a layer
	// opened in regional mode, so the viewport loads stay as they were.
	const HOME_NEA_IDS = ['misiones', 'corrientes', 'chaco', 'formosa'];
	const HOME_BBOX: [number, number, number, number] = (() => {
		const bb = HOME_NEA_IDS.map(id => TERRITORY_REGISTRY[id].bbox);
		return [Math.min(...bb.map(b => b[0])), Math.min(...bb.map(b => b[1])),
			Math.max(...bb.map(b => b[2])), Math.max(...bb.map(b => b[3]))];
	})();

	function getRegionalOtherPrefixes(countryFilter: CountryId | null, activePrefix: string): string[] {
		// Only neighbours where the open layer exists (3-nov-2026: analysis layers only in
		// NEA AR + Itapúa + Alto Paraná). Without this, Itapúa's regional neighbour was
		// Concepción and the map asked for parquets that are no longer in R2.
		const a = untrack(() => lensStore.activeAnalysis);
		return Object.values(TERRITORY_REGISTRY)
			.filter(t => t.available && (countryFilter === null || t.country === countryFilter))
			.filter(t => !a || a.coverage?.[t.id] !== 'unavailable')
			.map(t => t.parquetPrefix)
			.filter(p => p !== activePrefix);
	}

	function getRegionalBbox(countryFilter: CountryId | null): [number, number, number, number] {
		if (!countryFilter) return REGIONAL_NEA_BBOX;
		const ts = Object.values(TERRITORY_REGISTRY).filter(t => t.available && t.country === countryFilter);
		if (ts.length === 0) return REGIONAL_NEA_BBOX;
		return [
			Math.min(...ts.map(t => t.bbox[0])),
			Math.min(...ts.map(t => t.bbox[1])),
			Math.max(...ts.map(t => t.bbox[2])),
			Math.max(...ts.map(t => t.bbox[3])),
		];
	}

	// Start view: the map marks no territory and draws the whole-area outline.
	$effect(() => {
		const home = territoryStore.homeView;
		mapComponent?.setHomeView(home);
	});
	// The open layer's uncovered territories are not a pick (see territory-map-select).
	$effect(() => {
		const cov = lensStore.activeAnalysis?.coverage ?? {};
		mapComponent?.setUnpickableTerritories(Object.keys(cov).filter((k) => cov[k] === 'unavailable'));
	});
	// Choosing a lens loads the default territory's data: it is no longer the start view.
	$effect(() => {
		if (lensStore.activeLens) untrack(() => { territoryStore.homeView = false; });
	});

	// Sync territory prefix + fly map + reload data whenever territory changes
	// (and, through loadVisibleData's reads, on a layer change too).
	let prevTerritoryEffectId: string | null = null;
	$effect(() => {
		const t = territoryStore.activeTerritory;
		const isRegional = untrack(() => territoryStore.regionalMode);
		// Same territory, a department being worked in: a layer change reopens it with the camera
		// where the user left it (openDeptByName keepView), so no flight to the province here.
		const sameT = prevTerritoryEffectId === t.id;
		prevTerritoryEffectId = t.id;
		const keepCam = sameT && untrack(() => hexStore.workDept?.prefix === t.parquetPrefix);
		hexStore.setTerritoryPrefix(t.parquetPrefix);
		// BR territories: pre-fetch municipality boundaries (fire-and-forget).
		// First dept click after switch may render polygon outline with a brief lag
		// (typical 200-500ms on broadband); subsequent clicks are instant.
		if (t.id.endsWith('_br') || t.id.endsWith('_py')) ensureBrBoundaries(t.id);
		// Toggle territory-specific layers (mask, province boundary, buildings)
		mapComponent?.setActiveTerritory(t.id); // applyTerritoryVisibility() respects regionalModeActive
		// Reload hex choropleth for the new territory
		hexStore.loadVisibleData();
		// Reset selected dept since admin units differ per territory
		hexStore.selectedDpto = null;
		if (!isRegional) {
			// Normal mode: fly to territory + clear cross-territory selections
			// Skip fly when triggered by a building click (radios are selected) — don't zoom away from clicked point.
			const hasRadios = untrack(() => mapStore.selectedRadios.size > 0);
			if (!hasRadios && !keepCam) setTimeout(() => mapComponent?.flyToBbox(t.bbox), 100);
			mapStore.clearDistricts();
			mapComponent?.setDistrictHighlight([]);
		} else {
			// Regional mode with a per-department layer (the department ranking is of the
			// active province): fly to the province, not the country — at country zoom
			// the department lines (z ≥ 5 for the active province) were the only
			// reference the ranking had on the map, and they were off-screen.
			const perDeptLayer = untrack(() => hexStore.activeLayer?.perDepartment);
			const hasRadiosR = untrack(() => mapStore.selectedRadios.size > 0);
			if (perDeptLayer && !hasRadiosR && !keepCam) setTimeout(() => mapComponent?.flyToBbox(t.bbox), 150);
			// Regional mode: reload territories for current country filter
			untrack(() => hexStore.clearCompareDept());
			untrack(() => hexStore.clearRegionalData());
			const others = getRegionalOtherPrefixes(
				untrack(() => territoryStore.countryFilter),
				t.parquetPrefix
			);
			// 1 other (PY: Itapúa + Alto Paraná, the only PY layers): loadFullCompare bails on
			// perDepartment analyses, so route the single other to the regional
			// slot (loadGlobalInto, no perDepartment guard) and keep compare empty.
			if (others.length === 1) {
				untrack(() => hexStore.clearCompareDept());
				hexStore.loadRegionalData(others[0]).catch(() => {});
			} else if (others.length >= 2) {
				hexStore.loadFullCompare(others[0]).catch(() => {});
				hexStore.loadRegionalData(others[1]).catch(() => {});
			}
		}
	});

	// A click on the map, on a Brazilian building or on another territory's department
	// polygon can switch to a territory where the open layer does not exist (3-nov-2026:
	// analysis layers only in NEA AR + Itapúa + Alto Paraná). Close the layer, keep the
	// lens: the menu then shows it as unavailable there.
	$effect(() => {
		const tid = territoryStore.activeTerritory.id;
		const a = untrack(() => lensStore.activeAnalysis);
		if (a && a.coverage?.[tid] === 'unavailable') lensStore.clearAnalysis();
	});

	// Regional mode toggle + country filter: zoom to correct bbox, manage hex slots
	$effect(() => {
		const isRegional = territoryStore.regionalMode;
		const countryFilter = territoryStore.countryFilter;
		if (isRegional) {
			const hasRadios = untrack(() => mapStore.selectedRadios.size > 0);
			if (!hasRadios) {
				// Same rule as the territory effect: a per-department layer stays on the province.
				const bbox = untrack(() => hexStore.activeLayer?.perDepartment)
					? untrack(() => territoryStore.activeTerritory.bbox)
					: !countryFilter && !untrack(() => hexStore.activeLayer) ? HOME_BBOX
					: getRegionalBbox(countryFilter);
				setTimeout(() => mapComponent?.flyToBbox(bbox), 100);
			}
			mapComponent?.setRegionalMapMode(true);
			if (untrack(() => hexStore.activeLayer)) {
				const activePrefix = untrack(() => territoryStore.activeTerritory.parquetPrefix);
				const others = getRegionalOtherPrefixes(countryFilter, activePrefix);
				// 1 other (PY: Itapúa + Alto Paraná): route to regional slot —
				// loadFullCompare bails on perDepartment analyses (all 6 core are).
				if (others.length === 1) {
					untrack(() => hexStore.clearCompareDept());
					hexStore.loadRegionalData(others[0]).catch(() => {});
				} else if (others.length >= 2) {
					hexStore.loadFullCompare(others[0]).catch(() => {});
					hexStore.loadRegionalData(others[1]).catch(() => {});
				} else {
					untrack(() => hexStore.clearCompareDept());
					untrack(() => hexStore.clearRegionalData());
				}
			}
		} else {
			mapComponent?.setRegionalMapMode(false);
			mapComponent?.clearCompareHexChoropleth();
			mapComponent?.clearRegionalHexChoropleth();
			untrack(() => hexStore.clearCompareDept());
			untrack(() => hexStore.clearRegionalData());
			setTimeout(() => mapComponent?.flyToBbox(untrack(() => territoryStore.activeTerritory.bbox)), 100);
		}
	});

	onMount(() => {
		// Auto-collapse sidebar on mobile so map is visible first
		if (window.innerWidth < 768) {
			showAbout = false;
		}

		// Deep-link restore is gated on BOTH map style load and DuckDB init:
		// the state changes trigger effects that hit MapLibre (setPaintProperty
		// throws "Style is not done loading" mid-flush) and setLayer→
		// loadVisibleData no-ops if DuckDB isn't ready, with nothing to
		// re-trigger it (isReady() is non-reactive).
		const whenMapReady = new Promise<void>((resolve) => {
			if (mapReady) return resolve();
			mapContainer?.addEventListener('map-ready', () => resolve(), { once: true });
		});

		const whenDuckdb = initDuckDB()
			.then(() => {
				warmupRadioStats();
				// Cold-open without deep-link = the GBA buildings / population
				// canvas (no analysis pre-set): the only instant, universal,
				// trinational surface. The lens/analysis selector stays the
				// primary nav on top — the user picks a question from there.
			})
			.catch(e => { console.warn('DuckDB init failed:', e); duckdbFailed = true; });

		Promise.all([whenMapReady, whenDuckdb]).then(() => {
			if (duckdbFailed) return;
			// The tab's saved zones (sessionStorage) before the link's: a reload draws each once.
			hexStore.loadSavedZones();
			// Restore state from URL params (initialSearch: see capture note above)
			const params = new URLSearchParams(initialSearch);
			const cf = params.get('cf');
			if (cf === 'ar' || cf === 'py' || cf === 'br') territoryStore.enterCountryView(cf as CountryId);
			const t = params.get('t');
			if (t && TERRITORY_REGISTRY[t] && t !== territoryStore.activeTerritory.id) territoryStore.setTerritory(t);
			const lens = params.get('lens') as LensId | null;
			const analysisId = params.get('a');
			if (lens) {
				lensStore.setLens(lens);
				if (analysisId) {
					const a = getAnalysisById(analysisId);
					// Links shared before a layer left a territory (3-nov-2026: 15 Paraguayan
					// departments) would open it with a legend over an empty map.
					if (a && a.coverage?.[territoryStore.activeTerritory.id] !== 'unavailable') lensStore.setAnalysis(a);
				}
			}
			const tm = params.get('tm');
			// After tick: the analysis $effect calls setLayer, which resets
			// temporalMode to 'current' — setting tm before the flush gets wiped.
			if (tm === 'baseline' || tm === 'delta') tick().then(() => hexStore.setTemporalMode(tm));
			// Same reason as tm: setLayer resets scaleMode to 'provincial'.
			if (params.get('escala') === 'regional') tick().then(() => hexStore.setScaleMode('regional'));
			// Dept restore: after tick so the analysis/territory effects flushed
			// (setLayer and the territory effect both clear selectedDpto). The
			// dept summary gives back the parquetKey + centroid the click flow
			// would have passed.
			const dept = params.get('dept');
			// Zones of the link: drawn by hexStore.syncZones once this department has its cells.
			const urlZones = decodeZones(params.get('z'));
			if (dept && urlZones.length && analysisId && HEX_LAYER_REGISTRY[analysisId]) hexStore.pendingUrlZones = { dept, zones: urlZones, names: decodeZoneNames(params.get('zn')) };
			if (dept && analysisId && HEX_LAYER_REGISTRY[analysisId]) tick().then(() => openDeptByName(analysisId, dept, { keepMode: !!params.get('tm'), keepView: urlZones.length > 0 }));
		});

		// Option A: map-click territory switching
		mapContainer?.addEventListener('territory-map-select', ((e: CustomEvent) => {
			const { territory } = e.detail;
			if (!territory) return;
			// With a layer open, a territory where it does not exist is not a pick: switching
			// there only ends in «No se pudieron cargar los datos».
			if (lensStore.activeAnalysis?.coverage?.[territory] === 'unavailable') return;
			// From the start view the click also picks the default territory (Misiones).
			if (territory !== territoryStore.activeTerritory.id || territoryStore.homeView) {
				territoryStore.setTerritory(territory);
				// In regional mode the territory effect keeps the camera; a click on the
				// map is a pick, so frame it.
				setTimeout(() => mapComponent?.flyToBbox(TERRITORY_REGISTRY[territory].bbox), 120);
			}
		}) as EventListener);

		mapContainer?.addEventListener('radio-select', ((e: CustomEvent) => {
			if (lassoStore.active) return;
			const { redcode, selected, census } = e.detail;

			// Add radio FIRST so selectedRadios.size > 0 when the territory-change
			// effect fires — prevents flyToBbox from running on the first building click.
			showAbout = false;
			territoryStore.homeView = false; // a building click picks its territory, even the default one
			mapStore.addRadio(redcode, {
				census,
				enriched: null,
				buildingCount: selected.length
			});
			mapComponent?.setRadioHighlight(getRadioHighlightEntries());
			fetchRadioEnrichment(redcode);

			// Auto-switch territory AFTER addRadio so hasRadios guard in the effect is true
			const uf = String(redcode ?? '').slice(0, 2);
			const tid = territoryStore.activeTerritory.id;
			if (uf === '54' && tid !== 'misiones') {
				territoryStore.setTerritory('misiones');
			} else if (uf === '18' && tid !== 'corrientes') {
				territoryStore.setTerritory('corrientes');
			} else if (uf === '22' && tid !== 'chaco') {
				territoryStore.setTerritory('chaco');
			} else if (uf === '34' && tid !== 'formosa') {
				territoryStore.setTerritory('formosa');
			} else if (uf === '41' && tid !== 'parana_br') {
				territoryStore.setTerritory('parana_br');
			} else if (uf === '42' && tid !== 'santa_catarina_br') {
				territoryStore.setTerritory('santa_catarina_br');
			} else if (uf === '43' && tid !== 'rio_grande_sul_br') {
				territoryStore.setTerritory('rio_grande_sul_br');
			}
		}) as EventListener);

		mapContainer?.addEventListener('radio-deselect', ((e: CustomEvent) => {
			const { redcode } = e.detail;
			mapStore.removeRadio(redcode);
			mapComponent?.setRadioHighlight(getRadioHighlightEntries());
		}) as EventListener);

		mapContainer?.addEventListener('district-select', ((e: CustomEvent) => {
			showAbout = false;
			const { distrito, personas, territory } = e.detail;
			const targetTerritory = territory || 'itapua_py';
			// Do NOT switch primary territory on district select — district
			// selection/comparison is independent of the active hex/lens
			// territory. This avoids a heavy territory reload (the delay) and
			// the clearDistricts() wipe, and lets Itapúa + Alto Paraná
			// districts be compared together on the base map.
			mapStore.addDistrict(distrito, personas, targetTerritory);
			mapComponent?.setDistrictHighlight(getDistrictHighlightEntries());
			fetchDistrictEnrichment(distrito, targetTerritory);
		}) as EventListener);

		mapContainer?.addEventListener('district-deselect', ((e: CustomEvent) => {
			const { distrito } = e.detail;
			mapStore.removeDistrict(distrito);
			mapComponent?.setDistrictHighlight(getDistrictHighlightEntries());
		}) as EventListener);

		// Phase 2: click a department polygon → load that dept's hexes for the
		// active analysis. Resolves parquetKey/centroid from the analysis dept
		// summary by normalized-name match (handles AP caps / "L.G." vs "L. G.").
		mapContainer?.addEventListener('dept-map-select', (async (e: CustomEvent) => {
			const { name, territory } = e.detail;
			const t = TERRITORY_REGISTRY[territory];
			if (!t || !name) return;
			showAbout = false;
			if (territoryStore.activeTerritory.id !== territory) {
				territoryStore.setTerritory(territory);
				hexStore.setTerritoryPrefix(t.parquetPrefix);
			}
			const a = lensStore.activeAnalysis;
			if (!a) return;
			// Layers without per-department files (censo_temporal) have their own ranking
			// list: ask it to pick, as a click on its row would.
			const layer = HEX_LAYER_REGISTRY[a.id];
			if (layer && !layer.perDepartment) {
				hexStore.requestDeptPick(name, t.parquetPrefix);
				return;
			}
			const summary = await loadDeptSummary(a.id, t.parquetPrefix);
			const depts = summary?.departments as any[] | undefined;
			if (!depts?.length) return;
			const key = normDeptName(name);
			const entry = depts.find(d => normDeptName(d.dpto ?? d.distrito ?? d.municipio) === key);
			if (!entry?.parquetKey || !entry?.centroid) return;
			handleSelectFloodDpto(entry.dpto ?? entry.distrito ?? entry.municipio, entry.parquetKey, entry.centroid as [number, number]);
		}) as EventListener);

		mapContainer?.addEventListener('map-ready', (() => {
			mapReady = true;
			// Gancho de sondas del director (director/sondas): sólo con localStorage spatia_sonda=1.
			// Expone el estado real para comparar leyenda, pintura, tooltip y gráficos sin parsear config.ts.
			try {
				if (localStorage.getItem('spatia_sonda') === '1') {
					(window as any).__spatia = {
						map: mapComponent?.getMap(), hexStore, lensStore, i18n,
						registros: { TERRITORY_REGISTRY, HEX_LAYER_REGISTRY, ANALYSIS_REGISTRY }
					};
				}
			} catch { /* localStorage bloqueado: sin gancho */ }
		}) as EventListener);

		mapContainer?.addEventListener('compare-hex-select', ((e: CustomEvent) => {
			if (lassoStore.active) return;
			const { h3index } = e.detail;
			if (hexStore.activeLayer && hexStore.compareVisibleData.size > 0) {
				hexStore.selectCompareHex(h3index);
				hexStore.ensureProvincialAvgLoaded();
			}
		}) as EventListener);

		mapContainer?.addEventListener('regional-hex-select', ((e: CustomEvent) => {
			if (lassoStore.active) return;
			const { h3index } = e.detail;
			if (hexStore.activeLayer && hexStore.regionalVisibleData.size > 0) {
				hexStore.toggleRegionalHex(h3index);
				hexStore.ensureProvincialAvgLoaded();
			}
		}) as EventListener);

		mapContainer?.addEventListener('itapua-area-enter', (() => { itapuaHovering = true; }) as EventListener);
		mapContainer?.addEventListener('itapua-area-leave', (() => { itapuaHovering = false; }) as EventListener);

		mapContainer?.addEventListener('hex-select', ((e: CustomEvent) => {
			if (lassoStore.active) return;
			const { h3index, properties } = e.detail;
			// Auto-switch territory in regional mode when clicking a hex from a different territory
			if (territoryStore.regionalMode) {
				const detected = inferTerritoryFromH3(h3index);
				if (detected && detected !== territoryStore.activeTerritory.id) {
					territoryStore.setTerritory(detected);
				}
			}
			if (hexStore.activeLayer) {
				hexStore.toggleHex(h3index);
				// Ensure provincial avg is loaded for petal normalization
				hexStore.ensureProvincialAvgLoaded();
			} else {
				// Legacy single-hex mode (direct analysis)
				mapStore.setSelectedHex({
					h3index,
					jrc_occurrence: properties.jrc_occurrence,
					jrc_recurrence: properties.jrc_recurrence,
					jrc_seasonality: properties.jrc_seasonality,
					flood_extent_pct: properties.flood_extent_pct,
					...properties
				});
				mapComponent?.highlightHexagon(h3index);
				fetchHexData(h3index);
			}
		}) as EventListener);

		document.addEventListener('keydown', (e) => {
			// Typing in a field (a zone's name): Escape belongs to the field, not to the page reset.
			const tgt = e.target as HTMLElement | null;
			if (tgt && (tgt.tagName === 'INPUT' || tgt.tagName === 'TEXTAREA' || tgt.isContentEditable)) return;
			if (e.key === 'Escape') {
				if (lassoStore.drawing) {
					lassoStore.cancelDraw();
					mapComponent?.clearLassoDraw();
				} else if (lassoStore.active) {
					handleToggleLasso();
				} else if (hexStore.macizoChartFocus) {
					hexStore.setMacizoChartFocus(null);   // a macizo chart pick goes first, the department stays
				} else {
					clearAll();
				}
			}
		});

		// Lasso pointer events on the map container (mouse, pen and touch).
		// Coordinates are taken against the map canvas: e.offsetX/Y is relative to
		// whatever element is under the cursor, so a stroke that crossed the legend
		// or a panel jumped to another place of the map (fixed 7-nov-2026).
		const lassoLngLat = (e: PointerEvent): [number, number] | null => {
			const m = mapComponent?.getMap();
			if (!m) return null;
			const r = m.getCanvas().getBoundingClientRect();
			const ll = m.unproject([e.clientX - r.left, e.clientY - r.top]);
			return [ll.lng, ll.lat];
		};
		let lassoPointer: number | null = null;

		mapContainer?.addEventListener('pointerdown', (e: PointerEvent) => {
			if (!lassoStore.active || e.button !== 0 || lassoStore.drawing) return;
			// Only a stroke that starts on the map draws: a click on the legend, a
			// panel or the «Lazo» button itself is left to that element.
			const canvas = mapComponent?.getMap()?.getCanvas();
			if (!canvas || e.target !== canvas) return;
			e.stopPropagation();
			e.preventDefault();
			const p = lassoLngLat(e);
			if (!p) return;
			lassoPointer = e.pointerId;
			// Capture: the stroke keeps drawing and closes even if released off the map.
			try { canvas.setPointerCapture(e.pointerId); } catch { /* pointer already gone */ }
			lassoStore.startDraw();
			lassoStore.addPoint(p);
		}, { capture: true });

		mapContainer?.addEventListener('pointermove', (e: PointerEvent) => {
			if (!lassoStore.drawing || e.pointerId !== lassoPointer) return;
			e.stopPropagation();
			const p = lassoLngLat(e);
			if (!p) return;
			lassoStore.addPoint(p);
			mapComponent?.updateLassoDraw(lassoStore.currentPolygon);
		}, { capture: true });

		mapContainer?.addEventListener('pointercancel', (e: PointerEvent) => {
			if (e.pointerId !== lassoPointer) return;
			lassoPointer = null;
			lassoStore.cancelDraw();
			mapComponent?.clearLassoDraw();
		}, { capture: true });

		mapContainer?.addEventListener('pointerup', async (e: PointerEvent) => {
			if (!lassoStore.drawing || e.pointerId !== lassoPointer) return;
			lassoPointer = null;
			e.stopPropagation();
			const polygon = lassoStore.finishDraw();
			mapComponent?.clearLassoDraw();
			if (polygon.length < 4) return; // need at least 3 points + closing

			if (hexStore.activeLayer) {
				// Hex mode: find hexagons in polygon
				const h3indices = hexStore.findHexesInPolygon(polygon);
				if (h3indices.length === 0) return;
				await hexStore.createHexZone(h3indices, polygon, i18n.t('zone.origin.lasso'));
				updateHexZoneHighlights();
			} else {
				const redcodes = await lassoStore.findRadiosInPolygon(polygon);
				if (redcodes.length > 0) {
					// AR: real census radios → socioeconomic zone (unchanged).
					await lassoStore.createZone(redcodes, polygon);
					updateZoneHighlights();
				} else {
					const territory = territoryStore.activeTerritory.id;
					if (territory.endsWith('_br')) {
						// BR: find setores via rendered building features (tile has redcode)
						const brRedcodes = mapComponent?.queryBrSetoresInPolygon(polygon) ?? [];
						if (brRedcodes.length > 0) {
							await lassoStore.createBrZone(brRedcodes, polygon, territory);
							updateZoneHighlights();
						}
					} else {
						// PY (no census radios): select districts touched by lasso
						const districts = mapComponent?.queryDistrictsInPolygon(polygon) ?? [];
						for (const d of districts) {
							if (mapStore.hasDistrict(d.distrito)) continue;
							mapContainer?.dispatchEvent(new CustomEvent('district-select', {
								bubbles: true,
								detail: { distrito: d.distrito, personas: d.personas, territory: d.territory }
							}));
						}
					}
				}
			}
		}, { capture: true });
	});

	// ── Lens reactivity ──────────────────────────────────────────────────────
	let prevLensId: string | null = null;

	$effect(() => {
		const lens = lensStore.activeLens;
		if (lens === prevLensId) return;
		prevLensId = lens;

		// Clear hex state on any lens change
		mapComponent?.clearHexChoropleth();
		mapComponent?.clearCompareHexChoropleth();
		mapComponent?.clearHexZoneHighlight();
		mapStore.clearHexState();
		hexStore.clearAll();
		prevDataVersion = hexStore.dataVersion;
		prevCompareDataVersion = hexStore.compareDataVersion;

		if (lens) {
			mapStore.clearRadios();
			mapComponent?.clearRadioHighlight();
			// Lasso zones of the base map are census radios, not cells: no layer reads
			// them, and the sidebar put them above the lens's menu and layer.
			lassoStore.clearZones();
			mapComponent?.clearZoneHighlight();
			mapComponent?.clearAnalysisChoropleth();
		} else {
			mapComponent?.clearAnalysisChoropleth();
		}
	});

	function handleSelectAnalysis(analysis: AnalysisConfig) {
		showAbout = false;
		if (analysis.status === 'coming_soon') {
			// Still set it so AnalysisView shows the coming-soon card
			lensStore.setAnalysis(analysis);
			mapComponent?.clearAnalysisChoropleth();
			return;
		}
		lensStore.setAnalysis(analysis);

		// For analyses with choropleth, load will happen inside the component
		// The choropleth is triggered via the $effect below
	}

	// ── Analysis choropleth reactivity ──────────────────────────────────────
	let prevAnalysisId: string | null = null;
	let analysisDataLoaded = $state(false);
	let showAbout = $state(false);

	// Auto-open welcome panel on first visit after terms acceptance
	const WELCOMED_KEY = 'spatia_welcomed_v1';
	let hasShownWelcome = typeof window !== 'undefined'
		? localStorage.getItem(WELCOMED_KEY) === 'true'
		: true;
	// A link to a layer, cell or zone (shared on social media, mail) is not a first
	// look at the site: the welcome would cover the analysis it asked for, and the
	// panel that paints the departments would never mount. Skip it, and don't mark
	// the welcome as seen: it shows on that person's first visit to the home page.
	const DEEP_LINK_KEYS = ['a', 'h3', 'dept', 'zona'];
	const arrivedByDeepLink = DEEP_LINK_KEYS.some((k) => new URLSearchParams(initialSearch).has(k));
	$effect(() => {
		if (terms.accepted && !hasShownWelcome && !arrivedByDeepLink) {
			showAbout = true;
			hasShownWelcome = true;
			if (typeof window !== 'undefined') localStorage.setItem(WELCOMED_KEY, 'true');
		}
	});

	// hexStore resolves the per-territory primary fallback asynchronously (schema
	// check on the global parquet); refresh the hover tooltip unit when it lands so
	// a substituted primary (e.g. score instead of VIIRS) doesn't show the wrong unit.
	// Also on a tab change: the tab's column is what the map paints (economic_activity's
	// «Empleo» tab is %, its configured primary VIIRS is nW/cm²/sr — the tooltip kept the latter).
	$effect(() => {
		const pv = hexStore.effectivePrimary;
		const layerCfg = lensStore.activeAnalysis ? HEX_LAYER_REGISTRY[lensStore.activeAnalysis.id] : null;
		if (!layerCfg || hexStore.activeLayer?.id !== layerCfg.id) return;
		mapComponent?.setHexLayerInfo(i18n.t(layerCfg.titleKey), layerCfg.colorScale === 'categorical', getHexPrimaryUnit(layerCfg, pv));
	});


	$effect(() => {
		const analysis = lensStore.activeAnalysis;
		const id = analysis?.id ?? null;

		if (id === prevAnalysisId) return;
		prevAnalysisId = id;

		if (!id || !analysis) {
			mapComponent?.clearAnalysisChoropleth();
			mapComponent?.clearHexChoropleth();
			mapStore.clearHexState();
			hexStore.clearAll();
			analysisDataLoaded = false;
			return;
		}

		// Compare mode only makes sense on comparable layers — switching to a
		// non-comparable analysis already drops the compare data (setLayer), so
		// exit the mode too instead of leaving a dangling "Salir de comparación".
		if (!analysis.comparable && untrack(() => territoryStore.compareModeActive)) {
			territoryStore.exitCompareMode();
		}

		// Hex-based analyses use HexStore multi-resolution system
		if ((analysis.spatialUnit === 'hexagon' || analysis.spatialUnit === 'macizo') && (analysis.choropleth || HEX_LAYER_REGISTRY[analysis.id])) {
			analysisDataLoaded = false;
			mapComponent?.clearAnalysisChoropleth();
			const layerCfg = HEX_LAYER_REGISTRY[analysis.id];
			if (layerCfg) {
				// The department the user was working in (a layer change, not a fresh start):
				// the new layer opens there with the same camera instead of its ranking.
				const wd = untrack(() => hexStore.workDept);
				hexStore.setLayer(analysis.id);
				const tNow = untrack(() => territoryStore.activeTerritory);
				if (wd && wd.prefix === tNow.parquetPrefix && analysis.coverage?.[tNow.id] !== 'unavailable') {
					tick().then(() => openDeptByName(analysis.id, wd.name, { keepView: true }));
				}
				hexStore.ensureColorDomain().catch(() => {});
				mapStore.setActiveHexLayer(analysis.id);
				mapComponent?.setHexLayerInfo(i18n.t(layerCfg.titleKey), layerCfg.colorScale === 'categorical', getHexPrimaryUnit(layerCfg, untrack(() => hexStore.effectivePrimary)));
				// Regional mode: load data for other territories in scope
				if (untrack(() => territoryStore.regionalMode)) {
					const countryFilter = untrack(() => territoryStore.countryFilter);
					const activePrefix = untrack(() => territoryStore.activeTerritory.parquetPrefix);
					const others = getRegionalOtherPrefixes(countryFilter, activePrefix);
					if (others.length >= 1) hexStore.loadFullCompare(others[0]).catch(() => {});
					else untrack(() => hexStore.clearCompareDept());
					if (others.length >= 2) hexStore.loadRegionalData(others[1]).catch(() => {});
					else untrack(() => hexStore.clearRegionalData());
				}
			} else {
				// Fallback: direct load (legacy)
				loadHexChoropleth(analysis);
			}
			return;
		}

		// Radio-based analyses: load choropleth
		if (analysis.choropleth && analysis.spatialUnit === 'radio') {
			analysisDataLoaded = false;
			mapComponent?.clearHexChoropleth();
			mapStore.clearHexState();
			loadAnalysisChoropleth(analysis);
		}
	});

	// ── HexStore reactivity: sync hex selection highlights with map ───────
	$effect(() => {
		// Census layers: a click picks the cell's radio. Cells of one radio share its colour and the
		// radio is outlined (the panel shows its rates once; radioUnit.groupSelection).
		const tp = hexStore.territoryPrefix;
		const byRadio = radioSelectionOn(hexStore.activeLayer?.id, tp, hexStore.analysisResLevel != null)
			&& hexStore.compareVisibleData.size === 0;   // same gate as HexComparison
		if (byRadio) radioUnit.ensure(tp);
		const groups = byRadio ? radioUnit.groupSelection(tp, hexStore.selectedHexes) : null;
		const radioColor = new Map<string, string>();
		for (const r of groups?.radios ?? []) for (const h of r.hexes) radioColor.set(h, r.color);
		const allHexes = [...hexStore.selectedHexes.entries()].map(([h3index, d]) => ({
			h3index, color: radioColor.get(h3index) ?? d.color
		}));
		const primary = allHexes.filter(h => hexStore.visibleData.has(h.h3index));
		const compare = allHexes.filter(h => hexStore.compareVisibleData.has(h.h3index));
		mapComponent?.highlightHexagons(primary, compare);
		mapComponent?.setRadioSelected((groups?.radios ?? []).map(r => ({ redcode: r.redcode, color: r.color })));
	});

	// ── HexStore reactivity: re-render choropleth when data changes ──────
	// For perDepartment layers, rendering is handled directly by handleSelectFloodDpto.
	// This $effect only handles non-perDepartment layers (legacy path).
	let prevDataVersion = 0;
	let prevCompareDataVersion = 0;
	let prevRegionalDataVersion = 0;

	$effect(() => {
		const version = hexStore.dataVersion;
		const entries = hexStore.choroplethEntries;
		const layer = hexStore.activeLayer;

		if (version === prevDataVersion) return;
		prevDataVersion = version;

		// Skip for perDepartment layers — rendered directly via handleSelectFloodDpto
		if (layer?.perDepartment) return;

		if (entries.length === 0) {
			mapComponent?.clearHexChoropleth();
			return;
		}

		let colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer?.colorScale ?? 'flood';
		if (layer?.temporal && hexStore.temporalMode === 'delta') colorScale = 'diverging';
		mapComponent?.setHexChoropleth(entries, colorScale, hexStore.colorDomain ?? undefined, colorClassesFor(colorScale));
		analysisDataLoaded = true;
	});

	// ── Regional/compare viewport-loading (B) ────────────────────────────
	// Giant PY territories (Chaco/Boquerón 700K-850K hex) make a full-global load of the
	// regional/compare slots freeze. Instead, load only the H3 cells in the current viewport.
	// At res-9 the viewport explodes into 100K+ cells when zoomed out, so gate by zoom +
	// cell cap; below the gate the neighbor-territory hexes show empty (hint shown).
	const REGIONAL_ZOOM_MIN = 10;
	const REGIONAL_MAX_CELLS = 12000;
	// Giant (LOD) single-territory departments: above this zoom, swap the coarse overview
	// for real res-9 detail of the cells in view; below it, restore the coarse overview.
	const LOD_DETAIL_ZOOM_MIN = 11;
	const LOD_DETAIL_MAX_CELLS = 12000;
	const LOD_DETAIL_PADDED_MAX = 20000;
	let regionalViewportTooWide = $state(false);
	let mapReady = $state(false); // set on the map 'load' event; gates the viewport effect

	// LISA quadrants replace the layer's classes on the map: like the bivariate map,
	// the layer legend steps aside (the Moran panel carries the quadrant key).
	let lisaMapOn = $state(false);
	function handleShowLisa(entries: { h3index: string; value: number; boundary?: number[][] }[]) {
		lisaMapOn = entries.length > 0;
		if (entries.length === 0) {
			lastClassMap = null;
			prevDataVersion = 0;
			const layer = hexStore.activeLayer;
			const cEntries = hexStore.choroplethEntries;
			if (cEntries.length === 0) { mapComponent?.clearHexChoropleth(); return; }
			let cs: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer?.colorScale ?? 'flood';
			if (layer?.temporal && hexStore.temporalMode === 'delta') cs = 'diverging';
			mapComponent?.setHexChoropleth(cEntries, cs, hexStore.colorDomain ?? undefined, colorClassesFor(cs));
			return;
		}
		lastClassMap = { entries, scale: 'lisa' };
		paintClassMap();
	}

	// Bivariate 3×3 map: classes 1-9 painted over the layer; [] restores it.
	let bivariateMapOn = $state(false);
	function handleShowBivariateMap(entries: { h3index: string; value: number | null; nodata?: boolean }[]) {
		bivariateMapOn = entries.length > 0;
		if (entries.length === 0) { handleShowLisa([]); return; }
		lastClassMap = { entries, scale: 'bivariate' };
		paintClassMap();
	}

	function handleMoranBrush(h3s: string[], origin = '') { chartPick(h3s, origin); }

	function handleHistogramBrush(h3s: string[], origin = '') { chartPick(h3s, origin); }

	function handleRadioCensusBrush(redcodes: string[]) {
		if (redcodes.length === 0) {
			mapComponent?.clearRadioBrushHighlight();
			return;
		}
		mapComponent?.setRadioBrushHighlight(redcodes);
	}

	function handleBivariateBrush(h3s: string[], origin = '') { chartPick(h3s, origin); }

	function handleFlowBrush(h3s: string[], origin = '') { chartPick(h3s, origin); }

	// Charts select res-9 cells of the whole department; the map may be drawing the
	// coarse overview or a res-9 viewport. Project to what is drawn, and keep the last
	// selection / class map so a zoom (viewport swap) can re-apply them.
	let lastHighlight: { h3s: string[]; color: string } | null = null;
	let lastClassMap: { entries: { h3index: string; value: number | null; nodata?: boolean }[]; scale: 'lisa' | 'bivariate' } | null = null;
	$effect(() => {
		void hexStore.selectedDpto; void hexStore.activeLayer; // another dept/layer: forget them
		lastHighlight = null;
		lastClassMap = null;
	});
	function applyChartHighlight(h3s: string[], color: string) {
		lastHighlight = h3s.length === 0 ? null : { h3s, color };
		refreshHighlights();
	}
	// A chart selection: remember it (the panel offers «Guardar como zona») and paint it.
	function chartPick(h3s: string[], origin: string) {
		hexStore.setChartSelection(h3s, origin);
		applyChartHighlight(h3s, SELLO_HEX.cat[0]);
	}
	// Saved zones and the live chart selection share one map layer: draw both, the
	// selection last (on top). Cells are projected to what the map draws.
	function refreshHighlights() {
		const layers = hexStore.hexZones.map(z => ({ h3indices: hexStore.displayCells(z.h3indices), color: z.color }));
		if (lastHighlight) layers.push({ h3indices: hexStore.displayCells(lastHighlight.h3s), color: lastHighlight.color });
		if (layers.length === 0) mapComponent?.clearHexZoneHighlight();
		else mapComponent?.setHexZoneHighlight(layers, hexStore.boundaryCache);
		mapComponent?.setHexZoneLabels(hexStore.hexZones.map(z => ({ id: z.id, color: z.color, h3indices: z.h3indices, name: z.name })));
	}

	// Zones cross layers: when the open department's cells of a new layer arrive,
	// bring back the zones drawn in another layer and recompute them with this one.
	$effect(() => {
		void hexStore.dataVersion;
		void hexStore.analysisData;
		void hexStore.activeLayer;
		untrack(() => {
			hexStore.syncZones().then((changed) => {
				if (changed) refreshHighlights();
				// Zones of a shared link: the department view leaves them small in a corner.
				// Only once the zones of THAT department are on screen (a load in between keeps the request).
				const fk = hexStore.flyToZonesKey;
				if (fk && hexStore.hexZones.length && hexStore.zoneScopeKey() === fk) {
					hexStore.flyToZonesKey = null;
					let w = 180, s = 90, e = -180, n = -90;
					const grow = (x: number, y: number) => {
						if (x < w) w = x; if (x > e) e = x;
						if (y < s) s = y; if (y > n) n = y;
					};
					for (const z of hexStore.hexZones) {
						if (z.polygon) for (const [x, y] of z.polygon) grow(x, y);
						// Chart zones have no outline: their cells' centres.
						else for (const h of z.h3indices) { const [lat, lng] = cellToLatLng(h); grow(lng, lat); }
					}
					// Room for the lasso box (top left) and the legend (bottom); a phone gets a plain margin.
					const wide = (mapContainer?.clientWidth ?? 0) >= 700;
					const padding = wide ? { top: 70, bottom: 240, left: 190, right: 50 } : 30;
					if (w < e) mapComponent?.flyToBbox([w, s, e, n], { maxZoom: 14, pitch: 0, padding });
				}
			}).catch(() => {});
		});
	});
	async function handleSaveChartSelection(): Promise<string | null> {
		const sel = hexStore.chartSelection;
		if (!sel) return null;
		const id = await hexStore.createHexZone(sel.h3s, null, sel.origin);
		hexStore.chartSelection = null;
		lastHighlight = null;
		refreshHighlights();
		return id;
	}
	function handleClearChartSelection() {
		hexStore.chartSelection = null;
		lastHighlight = null;
		refreshHighlights();
	}
	function paintClassMap() {
		if (!lastClassMap) return;
		mapComponent?.setHexChoropleth(
			hexStore.displayEntries(lastClassMap.entries).map(e => ({ ...e, boundary: hexStore.boundaryCache.get(e.h3index) })),
			lastClassMap.scale
		);
	}

	// censo_temporal dept list: outline the picked department, fly to it and narrow
	// charts + map to its cells (hexStore.deptFilter). The
	// outline survives the selectedDpto $effect because censo_temporal never sets
	// selectedDpto; the list clears it (feature=null) on deselect/unmount.
	function handleCensoDeptSelect(feature: any | null) {
		mapComponent?.setDeptOutline(feature ?? null);
		mapComponent?.setHexClip(feature?.geometry ? feature : null);
		if (!feature?.geometry) { hexStore.setDeptFilter(null); return; }
		// Charts and map narrow to the department, as with the per-department layers.
		const outer: [number, number][][] = feature.geometry.type === 'Polygon'
			? [feature.geometry.coordinates[0]]
			: feature.geometry.coordinates.map((p: number[][][]) => p[0]);
		const cells = new Set<string>();
		for (const ring of outer) for (const h of hexStore.findHexesInPolygon(ring as [number, number][])) cells.add(h);
		hexStore.setDeptFilter(cells, feature.properties?.nombre ?? null);
		const keepView = keepViewOnce;
		keepViewOnce = false;
		if (keepView || hexStore.flyToZonesKey) return;
		let w = 180, s = 90, e = -180, n = -90;
		const polys = feature.geometry.type === 'Polygon'
			? [feature.geometry.coordinates]
			: feature.geometry.coordinates;
		for (const poly of polys) for (const ring of poly) for (const [x, y] of ring) {
			if (x < w) w = x; if (x > e) e = x;
			if (y < s) s = y; if (y > n) n = y;
		}
		mapComponent?.flyToBbox([w, s, e, n]);
	}

	// ── Compare choropleth: render compare dept hexes when compareDataVersion changes ──
	$effect(() => {
		const version = hexStore.compareDataVersion;
		const entries = hexStore.compareChoroplethEntries;
		const layer = hexStore.activeLayer;
		if (version === prevCompareDataVersion) return;
		prevCompareDataVersion = version;
		if (entries.length === 0) {
			mapComponent?.clearCompareHexChoropleth();
			return;
		}
		let colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer?.colorScale ?? 'sequential';
		mapComponent?.setCompareHexChoropleth(entries, colorScale, hexStore.colorDomain ?? undefined, colorClassesFor(colorScale));
	});

	// ── Regional choropleth: render 3rd territory hexes when regionalDataVersion changes ──
	$effect(() => {
		const version = hexStore.regionalDataVersion;
		const entries = hexStore.regionalChoroplethEntries;
		const layer = hexStore.activeLayer;
		if (version === prevRegionalDataVersion) return;
		prevRegionalDataVersion = version;
		if (entries.length === 0) {
			mapComponent?.clearRegionalHexChoropleth();
			return;
		}
		const colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer?.colorScale ?? 'sequential';
		mapComponent?.setRegionalHexChoropleth(entries, colorScale, hexStore.colorDomain ?? undefined, colorClassesFor(colorScale));
	});

	// ── Regional/compare viewport loading (B): refresh the neighbor-territory slots on
	// pan/zoom, loading ONLY the H3 cells in view. Active whenever a full-territory global
	// is wanted for either slot (regional mode, or a compare territory selected in the panel).
	$effect(() => {
		const comparePrefix = hexStore.compareGlobalPrefix;
		const regionalPrefix = hexStore.regionalGlobalPrefix;
		if (!mapReady) return; // re-runs when the map finishes loading (getMap() isn't reactive)
		const map = mapComponent?.getMap();
		if (!map) return;
		if (!comparePrefix && !regionalPrefix) {
			regionalViewportTooWide = false;
			return;
		}

		const refresh = () => {
			const m = mapComponent?.getMap();
			if (!m) return;
			const cp = hexStore.compareGlobalPrefix;
			const rp = hexStore.regionalGlobalPrefix;
			if (!cp && !rp) return;
			let cells: string[] = [];
			// Only enumerate cells above the zoom gate — at lower zoom a res-9 viewport would
			// be hundreds of thousands of cells (polygonToCells would itself be slow).
			if (m.getZoom() >= REGIONAL_ZOOM_MIN) {
				const b = m.getBounds();
				const rect = [[
					[b.getWest(), b.getSouth()], [b.getEast(), b.getSouth()],
					[b.getEast(), b.getNorth()], [b.getWest(), b.getNorth()],
					[b.getWest(), b.getSouth()],
				]];
				try { cells = polygonToCells(rect as any, 9, true); } catch { cells = []; }
				if (cells.length > REGIONAL_MAX_CELLS) cells = []; // too wide → clear + hint
			}
			regionalViewportTooWide = cells.length === 0;
			if (cp) hexStore.loadGlobalViewport('compare', cp, cells).catch(() => {});
			if (rp) hexStore.loadGlobalViewport('regional', rp, cells).catch(() => {});
		};

		// untrack: refresh() mutates store state (compareDataVersion++/regionalDataVersion++
		// inside loadGlobalViewport). Without untrack the effect would depend on the very
		// counters it writes → effect_update_depth_exceeded (infinite loop). The deps we want
		// are only the prefixes + mapReady, read above.
		untrack(() => refresh()); // initial load for the current view
		map.on('moveend', refresh);
		return () => map.off('moveend', refresh);
	});

	// ── LOD department viewport detail: swap coarse overview ↔ res-9 detail on zoom ──
	// A giant department renders an aggregated coarse overview (no main-thread freeze).
	// On zoom-in past the gate we re-query the res-9 cells in view for real per-hex detail;
	// on zoom-out we restore the cached coarse overview. perDepartment layers are skipped by
	// the primary render $effect, so we render here directly after the data swap.
	$effect(() => {
		const active = hexStore.lodDeptActive; // reactive: true while a giant dept is selected
		if (!mapReady || !active) return;
		const map = mapComponent?.getMap();
		if (!map) return;

		const refresh = () => {
			if (!hexStore.lodDeptActive) return;
			const m = mapComponent?.getMap();
			if (!m) return;
			let cells: string[] = [];
			if (m.getZoom() >= LOD_DETAIL_ZOOM_MIN) {
				const b = m.getBounds();
				const box = (pad: number) => {
					const dx = (b.getEast() - b.getWest()) * pad, dy = (b.getNorth() - b.getSouth()) * pad;
					const w = b.getWest() - dx, e = b.getEast() + dx, s = b.getSouth() - dy, n = b.getNorth() + dy;
					return [[[w, s], [e, s], [e, n], [w, n], [w, s]]];
				};
				// A margin around the view: small pans/zooms land on cells already loaded
				// (no blank edges while the next query runs). Too many → just the view.
				try { cells = polygonToCells(box(0.3) as any, 9, true); } catch { cells = []; }
				if (cells.length > LOD_DETAIL_PADDED_MAX) {
					try { cells = polygonToCells(box(0) as any, 9, true); } catch { cells = []; }
				}
				if (cells.length > LOD_DETAIL_MAX_CELLS) cells = []; // too wide → keep coarse
			}
			hexStore.loadDeptViewport(cells).then((changed) => {
				if (!changed || !hexStore.activeLayer) return;
				// One repaint: an active class map (3×3, Moran quadrants) is drawn directly at
				// the new resolution instead of the layer choropleth followed by it (flicker).
				if (lastClassMap) paintClassMap();
				else {
					const entries = hexStore.choroplethEntries;
					if (entries.length === 0) return;
					let cs: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = hexStore.activeLayer.colorScale ?? 'flood';
					if (hexStore.activeLayer.temporal && hexStore.temporalMode === 'delta') cs = 'diverging';
					mapComponent?.setHexChoropleth(entries, cs, hexStore.colorDomain ?? undefined, colorClassesFor(cs));
				}
				if (lastHighlight || hexStore.hexZones.length) refreshHighlights();
			}).catch(() => {});
		};

		// untrack: refresh() mutates dataVersion via loadDeptViewport → without untrack the
		// effect would depend on the counter it writes → effect_update_depth_exceeded.
		untrack(() => refresh());
		map.on('moveend', refresh);
		return () => map.off('moveend', refresh);
	});

	// ── Clear data load error when analysis changes ──────────────────────────
	$effect(() => {
		lensStore.activeAnalysis; // reactive dependency
		hexStore.clearLoadError();
	});

	// Census radios are census/base-mode only — hide them whenever a hex
	// analysis is active so the deforestation cold-open general view isn't
	// covered by radio polygons before zooming to the department.
	$effect(() => {
		mapStore.activeHexLayer; // reactive dependency
		mapComponent?.refreshCensusVisibility();
	});

	// Phase 2: the department-picker polygons are the selection surface — shown
	// when an analysis is active and no department is selected yet; hidden once
	// a department's hexes are loaded.
	$effect(() => {
		const a = lensStore.activeAnalysis;
		const dpto = hexStore.selectedDpto;
		// A by-department reading (hexHidden) keeps the departments on the map with one open.
		const show = !!a && (!dpto || hexStore.hexHidden);
		mapComponent?.setDeptPickerVisible(show);
		// Lazy-fill the AR dept polygons (deferred out of the initial bundle) once the
		// picker is shown for an AR territory — PY/BR use their own district layers.
		if (show && isArDeptTerritory(hexStore.territoryPrefix)) mapComponent?.loadArDeptSource();
		// Pre-fetch colorDomain so first dept click doesn't wait for the global parquet query
		if (a?.perDepartment) hexStore.ensureColorDomain().catch(() => {});
	});

	// Province view: paint the departments with the ranking reading the panel shows.
	$effect(() => {
		const f = hexStore.mapDeptFill;
		mapComponent?.setDeptFill(f);
	});
	// By-department reading with a department open: its hexagons are hidden (no clicks either).
	$effect(() => {
		const h = hexStore.hexHidden;
		mapComponent?.setHexHidden(h);
	});

	// Macizo layers (forestry_harvest): the plantation tiles carry the data; paint the tab's property with its
	// classes, all the territory's macizos or only the open department's.
	$effect(() => {
		const layer = hexStore.activeLayer;
		const prop = hexStore.effectivePrimary;
		const classes = hexStore.effectiveColorClasses;
		const deptKey = hexStore.selectedParquetKey;
		const focus = hexStore.macizoPaintFocus;
		const pick = !!hexStore.macizoChartFocus;
		mapComponent?.setMacizoPaint(layer?.macizos && classes ? { prop, classes, deptKey, focus, pick } : null);
	});

	// Census rates: outline the open department's INDEC radios (the unit the rate is measured on)
	// and the hovered one. Not in compare mode, not for satellite layers or cell-support variables.
	$effect(() => {
		const tp = hexStore.territoryPrefix;
		const key = hexStore.selectedParquetKey;
		const codprov = codprovOf(tp);
		const on = !!key && !!codprov && !territoryStore.compareModeActive && !hexStore.hexHidden
			&& isRadioSupport(hexStore.activeLayer?.id, hexStore.effectivePrimary);
		if (on) radioUnit.ensure(tp);
		mapComponent?.setRadioUnit(on ? { prefix: tp, deptKey: key, codprov: codprov! } : null);
	});

	// ── Dept bbox outlines + auto-fly when both depts are loaded ────────────
	// Primary + compare bbox rectangles are only rendered as a fallback when no real
	// polygon exists (e.g. Itapúa, PY — no polygons in ar_dept_boundaries.json).
	// For AR depts, the real polygon is rendered separately via dept-outline / compare-dept-outline.
	$effect(() => {
		const p = hexStore.deptBbox;
		const c = hexStore.compareDeptBbox;
		const dpto = hexStore.selectedDpto;
		const tp = hexStore.territoryPrefix;
		const compareDpto = hexStore.compareDpto;
		const compareTp = hexStore.compareTerritoryPrefix;
		const primaryHasPolygon = !!(dpto && findDeptFeature(dpto, tp));
		const compareHasPolygon = !!(compareDpto && compareTp != null
			&& findDeptFeature(compareDpto, compareTp));
		// Primary (celeste) highlight only in compare mode — in single-dept selection it's
		// redundant with the dept's hexagons. Compare (amber) is kept as the comparison marker.
		const inCompare = territoryStore.compareModeActive;
		mapComponent?.updateDeptHighlights(
			inCompare ? (primaryHasPolygon ? null : p) : null,
			compareHasPolygon ? null : c
		);
		if (p && c) {
			const union: [number, number, number, number] = [
				Math.min(p[0], c[0]),
				Math.min(p[1], c[1]),
				Math.max(p[2], c[2]),
				Math.max(p[3], c[3])
			];
			mapComponent?.flyToBbox(union);
		}
	});

	// ── Selected department real-polygon outline (primary + compare) ───────
	$effect(() => {
		const dpto = hexStore.selectedDpto;
		const tp = hexStore.territoryPrefix;
		const feat = dpto ? findDeptFeature(dpto, tp) : null;
		// The celeste outline of the selected dept is redundant with its hexagons, and the
		// generalized admin line visibly cuts across rivers on the satellite basemap → only
		// draw it in compare mode (to mark the primary vs the amber compared dept).
		// Census layers are the exception: they are dasymetric (hexes only where people
		// live), so below the LOD gate a rural dept is scattered dots with no visible edge.
		const sparseLayer = lensStore.activeAnalysis?.rigorBadge === 'census';
		// A by-department reading (hexHidden) paints the departments: the outline marks the open one.
		mapComponent?.setDeptOutline(territoryStore.compareModeActive || sparseLayer || hexStore.hexHidden ? feat : null);
		// Every layer: hexagons cut at the official limit (the line, drawn or not).
		mapComponent?.setHexClip(feat);

		const compareDpto = hexStore.compareDpto;
		const compareTp = hexStore.compareTerritoryPrefix;
		const compareFeat = compareDpto && compareTp != null
			? findDeptFeature(compareDpto, compareTp)
			: null;
		mapComponent?.setCompareDeptOutline(compareFeat);
		mapComponent?.setCompareHexClip(compareFeat);
	});

	// ── Clear compare dept when compare mode is exited ───────────────────
	$effect(() => {
		if (!territoryStore.compareModeActive) {
			untrack(() => hexStore.clearCompareDept());
		}
	});

	// ── Temporal mode toggle: re-render perDepartment layers on mode change ──
	let prevTemporalMode: string = 'current';
	$effect(() => {
		const mode = hexStore.temporalMode;
		const layer = hexStore.activeLayer;
		if (mode === prevTemporalMode) return;
		prevTemporalMode = mode;
		if (!layer?.temporal || !layer?.perDepartment) return;
		const entries = hexStore.choroplethEntries;
		if (entries.length === 0) return;
		let colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer.colorScale ?? 'flood';
		if (mode === 'delta') colorScale = 'diverging';
		mapComponent?.setHexChoropleth(entries, colorScale, hexStore.colorDomain ?? undefined, colorClassesFor(colorScale));
	});

	// ── Scale toggle (provincial P2–P98 ↔ regional fixed): new domain, re-render ──
	let prevScaleMode: string = 'provincial';
	$effect(() => {
		const mode = hexStore.scaleMode;
		if (mode === prevScaleMode) return;
		prevScaleMode = mode;
		untrack(() => rerenderForScale());
	});

	// ── Painted variable changed (flood: the active ranking tab) → repaint ──
	let prevColorVariable: string | null = null;
	$effect(() => {
		const v = hexStore.colorVariable;
		if (v === prevColorVariable) return;
		prevColorVariable = v;
		untrack(() => rerenderForVariable());
	});

	async function rerenderForVariable() {
		if (!hexStore.activeLayer) return;
		if (!hexStore.effectiveColorClasses) await hexStore.ensureColorDomain();
		repaintAll();
	}

	async function rerenderForScale() {
		const layer = hexStore.activeLayer;
		if (!layer?.regionalColorDomain) return;
		await hexStore.ensureColorDomain();
		repaintAll();
	}

	function repaintAll() {
		const layer = hexStore.activeLayer;
		if (!layer) return;
		let cs: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = layer.colorScale ?? 'flood';
		if (layer.temporal && hexStore.temporalMode === 'delta') cs = 'diverging';
		const domain = hexStore.colorDomain ?? undefined;
		// Same province-bounding as the dept render (Misiones needs PIP).
		const all = hexStore.choroplethEntries;
		const entries = hexStore.territoryPrefix === ''
			? all.filter((e) => {
				if (e.nodata) return true;
				const [lat, lng] = cellToLatLng(e.h3index);
				return isInsideActiveTerritory(lat, lng, hexStore.territoryPrefix);
			})
			: all;
		if (entries.length > 0) mapComponent?.setHexChoropleth(entries, cs, domain, colorClassesFor(cs));
		const cmp = hexStore.compareChoroplethEntries;
		if (cmp.length > 0) mapComponent?.setCompareHexChoropleth(cmp, cs, domain, colorClassesFor(cs));
		const reg = hexStore.regionalChoroplethEntries;
		if (reg.length > 0) mapComponent?.setRegionalHexChoropleth(reg, cs, domain, colorClassesFor(cs));
	}

	async function loadAnalysisChoropleth(analysis: AnalysisConfig) {
		if (!analysis.choropleth || !isReady()) return;
		try {
			const { getParquetUrl } = await import('$lib/config');
			const url = getParquetUrl(analysis.choropleth.parquet);
			const col = analysis.choropleth.column;
			const result = await query(
				`SELECT redcode, CAST(${col} AS DOUBLE) as value FROM '${url}' WHERE ${col} IS NOT NULL AND ${col} > 0`
			);
			const entries: Array<{ redcode: string; value: number }> = [];
			for (let i = 0; i < result.numRows; i++) {
				const row = result.get(i)!.toJSON() as { redcode: string; value: number };
				entries.push(row);
			}
			mapComponent?.setAnalysisChoropleth(entries, analysis.choropleth.colorScale);
			analysisDataLoaded = true;
		} catch (e) {
			console.warn('Failed to load analysis choropleth:', e);
		}
	}

	function inferTerritoryFromH3(h3index: string): string | null {
		const [lat, lng] = cellToLatLng(h3index);
		if (isInsideMisiones(lat, lng)) return 'misiones';
		if (isInsideItapua(lat, lng)) return 'itapua_py';
		if (isInsideAltoParana(lat, lng)) return 'alto_parana_py';
		if (isInsideCorrientes(lat, lng)) return 'corrientes';
		if (isInsideChaco(lat, lng)) return 'chaco';
		if (isInsideFormosa(lat, lng)) return 'formosa';
		if (isInsideParanaBr(lat, lng)) return 'parana_br';
		if (isInsideSantaCatarinaBr(lat, lng)) return 'santa_catarina_br';
		if (isInsideRioGrandeSulBr(lat, lng)) return 'rio_grande_sul_br';
		return null;
	}

	// territoryPrefix → PIP. Used by per-territory hex filtering so we don't
	// silently fall back to isInsideMisiones() and drop every BR/CHA/FOR hex.
	function isInsideActiveTerritory(lat: number, lng: number, territoryPrefix: string): boolean {
		switch (territoryPrefix) {
			case 'itapua_py/':              return isInsideItapua(lat, lng);
			case 'alto_parana_py/':         return isInsideAltoParana(lat, lng);
			case 'corrientes/':             return isInsideCorrientes(lat, lng);
			case 'chaco/':                  return isInsideChaco(lat, lng);
			case 'formosa/':                return isInsideFormosa(lat, lng);
			case 'parana_br/':              return isInsideParanaBr(lat, lng);
			case 'santa_catarina_br/':      return isInsideSantaCatarinaBr(lat, lng);
			case 'rio_grande_sul_br/':      return isInsideRioGrandeSulBr(lat, lng);
			// New Paraguay departments
			case 'concepcion_py/':          return isInsideConcepcion(lat, lng);
			case 'san_pedro_py/':           return isInsideSanPedro(lat, lng);
			case 'cordillera_py/':          return isInsideCordillera(lat, lng);
			case 'guaira_py/':              return isInsideGuaira(lat, lng);
			case 'caaguazu_py/':            return isInsideCaaguazu(lat, lng);
			case 'caazapa_py/':             return isInsideCaazapa(lat, lng);
			case 'misiones_py/':            return isInsideMisionesPy(lat, lng);
			case 'paraguari_py/':           return isInsideParaguari(lat, lng);
			case 'central_py/':             return isInsideCentral(lat, lng);
			case 'neembucu_py/':            return isInsideNeembucu(lat, lng);
			case 'amambay_py/':             return isInsideAmambay(lat, lng);
			case 'canindeyu_py/':           return isInsideCanindeyu(lat, lng);
			case 'presidente_hayes_py/':    return isInsidePresidenteHayes(lat, lng);
			case 'boqueron_py/':            return isInsideBoqueron(lat, lng);
			case 'alto_paraguay_py/':       return isInsideAltoParaguay(lat, lng);
			default:                        return isInsideMisiones(lat, lng); // '' = Misiones
		}
	}

	async function fetchHexData(h3index: string): Promise<void> {
		if (!isReady()) return;
		try {
			const result = await query(
				`SELECT h3index, jrc_occurrence, jrc_recurrence, jrc_seasonality, flood_extent_pct
				 FROM '${PARQUETS.hex_flood_risk}'
				 WHERE h3index = '${h3index.replace(/'/g, "''")}'
				 LIMIT 1`
			);
			if (result.numRows > 0) {
				const row = result.get(0)!.toJSON();
				mapStore.setSelectedHex(row as any);
			}
		} catch (e) {
			console.warn('Failed to fetch hex data:', e);
		}
	}

	async function loadHexChoropleth(analysis: AnalysisConfig) {
		if (!analysis.choropleth || !isReady()) return;
		try {
			const { getParquetUrl } = await import('$lib/config');
			const url = getParquetUrl(analysis.choropleth.parquet);
			const col = analysis.choropleth.column;
			const result = await query(
				`SELECT h3index, ${col} as value FROM '${url}' WHERE ${col} IS NOT NULL AND ${col} > 0`
			);
			const entries: Array<{ h3index: string; value: number }> = [];
			for (let i = 0; i < result.numRows; i++) {
				const row = result.get(i)!.toJSON() as { h3index: string; value: number };
				entries.push(row);
			}
			mapComponent?.setHexChoropleth(entries, analysis.choropleth.colorScale as any);
			mapStore.setActiveHexLayer(analysis.id);
			analysisDataLoaded = true;
		} catch (e) {
			console.warn('Failed to load hex choropleth:', e);
		}
	}

	async function warmupRadioStats(): Promise<void> {
		try {
			await query(`SELECT 1 FROM '${PARQUETS.radio_stats_master}' LIMIT 1`);
		} catch (_) { /* non-critical */ }
	}

	async function fetchRadioEnrichment(redcode: string): Promise<void> {
		await initDuckDB();
		if (!/^\d{9,15}$/.test(redcode)) return;
		try {
			const uf = redcode.slice(0, 2);
			// Brazil: IBGE setor code (15 digits, UF prefix 41/42/43)
			if (uf === '41' || uf === '42' || uf === '43') {
				const parquet = uf === '41' ? PARQUETS.radio_stats_parana_br
				              : uf === '42' ? PARQUETS.radio_stats_santa_catarina_br
				              :               PARQUETS.radio_stats_rio_grande_sul_br;
				const result = await query(
					`SELECT * FROM '${parquet}' WHERE redcode = '${redcode}' LIMIT 1`
				);
				if (result.numRows === 0) return;
				mapStore.updateEnriched(redcode, result.get(0)!.toJSON());
				return;
			}
			// Argentina: INDEC radio code (9 digits)
			const isCorrientes = uf === '18';
			const isChaco      = uf === '22';
			const isFormosa    = uf === '34';
			if (isCorrientes) {
				const CORRIENTES_COLS = 'redcode, total_personas, area_km2, tasa_actividad, tasa_empleo, pct_universitario, pct_nbi, pct_hacinamiento';
				const result = await query(
					`SELECT ${CORRIENTES_COLS} FROM '${PARQUETS.radio_stats_corrientes}' WHERE redcode = '${redcode}' LIMIT 1`
				);
				if (result.numRows === 0) return;
				mapStore.updateEnriched(redcode, result.get(0)!.toJSON());
			} else if (isChaco) {
				const result = await query(
					`SELECT * FROM '${PARQUETS.radio_stats_chaco}' WHERE redcode = '${redcode}' LIMIT 1`
				);
				if (result.numRows === 0) return;
				mapStore.updateEnriched(redcode, result.get(0)!.toJSON());
			} else if (isFormosa) {
				const result = await query(
					`SELECT * FROM '${PARQUETS.radio_stats_formosa}' WHERE redcode = '${redcode}' LIMIT 1`
				);
				if (result.numRows === 0) return;
				mapStore.updateEnriched(redcode, result.get(0)!.toJSON());
			} else {
				const ENRICHMENT_COLS = 'redcode, total_personas, area_km2, tasa_actividad, tasa_empleo, pct_universitario, pct_nbi, pct_hacinamiento, pct_agua_red';
				const result = await query(
					`SELECT ${ENRICHMENT_COLS} FROM '${PARQUETS.radio_stats_master}' WHERE redcode = '${redcode}' LIMIT 1`
				);
				if (result.numRows === 0) return;
				mapStore.updateEnriched(redcode, result.get(0)!.toJSON());
			}
		} catch (e) {
			console.warn('DuckDB enrichment failed:', e);
		}
	}

	// ── Radio export handlers (CSV / GeoJSON / summary) ─────────────────────
	async function downloadRadioCsv(redcode: string): Promise<void> {
		const { downloadCsvFromQuery } = await import('$lib/utils/data-export');
		await downloadCsvFromQuery(
			`SELECT * FROM '${PARQUETS.radio_stats_master}' WHERE redcode = '${redcode}' LIMIT 1`,
			`spatia_radio_${redcode}.csv`
		);
	}

	function downloadRadioGeoJson(redcode: string, properties: Record<string, any>): void {
		const geometry = mapComponent?.getRadioGeometry(redcode);
		if (!geometry) {
			console.warn('Radio geometry not available for', redcode);
			return;
		}
		const fc = {
			type: 'FeatureCollection',
			features: [{ type: 'Feature', geometry, properties: { redcode, ...properties } }]
		};
		const blob = new Blob([JSON.stringify(fc)], { type: 'application/geo+json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `spatia_radio_${redcode}.geojson`;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		URL.revokeObjectURL(url);
	}

	// PY districts have no vector-tile geometry usable for export (tiles clip
	// the large polygons into seams). Their full unclipped polygons ship as a
	// static asset generated by pipeline/build_itapua_districts.py --export-geojson.
	const districtGeoCache = new Map<string, any>();
	async function downloadDistrictGeoJson(
		distrito: string,
		territory: string,
		properties: Record<string, any>
	): Promise<void> {
		const stem = territory === 'alto_parana_py' ? 'alto_parana' : 'itapua';
		let fc = districtGeoCache.get(stem);
		if (!fc) {
			try {
				const res = await fetch(`/data/${stem}_districts.geojson`);
				if (!res.ok) throw new Error(`HTTP ${res.status}`);
				fc = await res.json();
				districtGeoCache.set(stem, fc);
			} catch (e) {
				console.warn('District polygons fetch failed for', stem, e);
				return;
			}
		}
		const feature = fc.features?.find((f: any) => f.properties?.district === distrito);
		if (!feature) {
			console.warn('District geometry not available for', distrito, `(${stem})`);
			return;
		}
		const out = {
			type: 'FeatureCollection',
			features: [{ type: 'Feature', geometry: feature.geometry, properties: { distrito, ...properties } }]
		};
		const safe = distrito.replace(/[^\p{L}\p{N}]+/gu, '_').replace(/^_+|_+$/g, '') || 'distrito';
		const blob = new Blob([JSON.stringify(out)], { type: 'application/geo+json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `spatia_distrito_${safe}.geojson`;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		URL.revokeObjectURL(url);
	}

	async function downloadRadiosSummary(): Promise<void> {
		const { downloadCsvFromRows } = await import('$lib/utils/data-export');
		const rows: Array<Record<string, unknown>> = [];
		for (const [rc, d] of mapStore.selectedRadios.entries()) {
			const e = d.enriched ?? {};
			rows.push({
				redcode: rc,
				total_personas: e.total_personas,
				area_km2: e.area_km2,
				tasa_actividad: e.tasa_actividad,
				tasa_empleo: e.tasa_empleo,
				pct_universitario: e.pct_universitario,
				pct_nbi: e.pct_nbi,
				pct_hacinamiento: e.pct_hacinamiento,
				pct_agua_red: e.pct_agua_red
			});
		}
		if (rows.length === 0) return;
		const cols = Object.keys(rows[0]);
		downloadCsvFromRows(rows, cols, 'spatia_radios_summary.csv');
	}

	async function fetchRadioCensus(redcode: string): Promise<Record<string, any>> {
		await initDuckDB();
		if (!/^\d{9}$/.test(redcode)) return {};
		try {
			const result = await query(
				`SELECT * FROM '${PARQUETS.radio_stats_master}' WHERE redcode = '${redcode}' LIMIT 1`
			);
			if (result.numRows === 0) return {};
			return result.get(0)!.toJSON();
		} catch (e) {
			console.warn('DuckDB census fetch failed:', e);
			return {};
		}
	}

	function getRadioHighlightEntries(): Array<{redcode: string, color: string}> {
		return [...mapStore.selectedRadios.entries()].map(([rc, d]) => ({redcode: rc, color: d.color}));
	}

	function getDistrictHighlightEntries(): Array<{distrito: string, color: string, territory: string}> {
		return [...mapStore.selectedDistricts.entries()].map(([d, data]) => ({distrito: d, color: data.color, territory: data.territory}));
	}

	async function fetchDistrictEnrichment(distrito: string, territory: string = 'itapua_py'): Promise<void> {
		try {
			await initDuckDB();
			// Only Itapúa and Alto Paraná have district stats: elsewhere a homonym district
			// (Bella Vista is in Itapúa and in Amambay) would show Itapúa's figures.
			if (territory !== 'itapua_py' && territory !== 'alto_parana_py') return;
			const statsUrl = territory === 'alto_parana_py'
				? PARQUETS.district_stats_alto_parana
				: PARQUETS.district_stats_itapua;
			const result = await query(`SELECT * FROM '${statsUrl}' WHERE distrito = '${distrito.replace(/'/g, "''")}' LIMIT 1`);
			if (result.numRows > 0) {
				mapStore.updateDistrictEnriched(distrito, result.get(0)!.toJSON() as Record<string, any>);
			}
		} catch (e) {
			console.warn('District enrichment fetch failed:', e);
		}
	}

	function handleRemoveDistrict(distrito: string) {
		mapStore.removeDistrict(distrito);
		mapComponent?.setDistrictHighlight(getDistrictHighlightEntries());
	}

	function handleClearDistricts() {
		mapStore.clearDistricts();
		mapComponent?.setDistrictHighlight([]);
	}

	function handleRemoveRadio(redcode: string) {
		mapStore.removeRadio(redcode);
		mapComponent?.setRadioHighlight(getRadioHighlightEntries());
	}

	function handleClearRadios() {
		mapStore.clearRadios();
		mapComponent?.setRadioHighlight([]);
	}

	// ── Lasso handlers ──────────────────────────────────────────────────────

	function handleToggleLasso() {
		lassoStore.toggle();
		// Warm the centroids chunk while the user draws, so findRadiosInPolygon
		// (awaited on mouseup) is instant. Swallow: the mouseup await retries.
		if (lassoStore.active) lassoStore.ensureCentroids().catch(() => {});
		mapComponent?.setLassoMode(lassoStore.active);
	}

	function updateZoneHighlights() {
		const zones = lassoStore.zones.map(z => ({ redcodes: z.redcodes, color: z.color }));
		mapComponent?.setZoneHighlight(zones);
	}

	function handleRemoveZone(id: string) {
		lassoStore.removeZone(id);
		updateZoneHighlights();
	}

	function handleClearZones() {
		lassoStore.clearZones();
		mapComponent?.clearZoneHighlight();
	}

	// ── Hex selection + zone helpers ─────────────────────────────────────

	function updateHexZoneHighlights() { refreshHighlights(); }

	function handleRemoveHexZone(id: string) {
		hexStore.removeHexZone(id);
		updateHexZoneHighlights();
	}

	function handleClearHexZones() {
		hexStore.clearHexZones();
		refreshHighlights();
	}


	// Monotonic token: when a newer dept is selected while a load is in progress,
	// the stale load's render is abandoned (no blank map flash, no wrong data).
	let _deptToken = 0;
	// Debounce timer: rapid clicks collapse into a single load after 80ms of inactivity.
	let _deptDebounceTimer: ReturnType<typeof setTimeout> | null = null;

	// Discrete classes of the active layer (flood: JRC occurrence). Delta mode
	// renders a signed change, so the level classes don't apply there.
	function colorClassesFor(colorScale: string) {
		const layer = hexStore.activeLayer;
		// Delta mode paints 'diverging'; effectiveColorClasses then holds only the layer's deltaClasses.
		const delta = !!layer?.temporal && hexStore.temporalMode === 'delta' && colorScale === 'diverging';
		return layer && (colorScale === layer.colorScale || delta) ? hexStore.effectiveColorClasses : undefined;
	}

	// Set by a layer change that reopens the working department: the camera stays where the
	// user was (Posadas centre stays Posadas centre), the department load does not refit it.
	let keepViewOnce = false;

	/** Open a department by name in a layer: per-department layers through their summary
	 *  (parquetKey + centroid, as a ranking click), territory-wide ones (censo_temporal)
	 *  through their own list once the layer's cells are in. Used by ?dept=, by a layer
	 *  change with a department open (hexStore.workDept) and by «ir» on a saved zone.
	 *  False when the layer has no such department (its ranking stays). */
	async function openDeptByName(analysisId: string, dept: string, opts: { keepMode?: boolean; keepView?: boolean } = {}): Promise<boolean> {
		const layer = HEX_LAYER_REGISTRY[analysisId];
		if (!layer) return false;
		if (layer.perDepartment) {
			const summary = await loadDeptSummary(analysisId, hexStore.territoryPrefix);
			if (lensStore.activeAnalysis?.id !== analysisId) return false;
			const deps = summary?.departments as any[] | undefined;
			const nameOf = (x: any) => x.dpto ?? x.distrito ?? x.municipio;
			// Links shared before 2-nov-2026 carry GADM spellings ("Nueve de Julio");
			// summaries now use INDEC's ("9 de Julio"): fall back to the normalized key.
			const d = deps?.find((x) => nameOf(x) === dept)
				?? deps?.find((x) => normDeptName(nameOf(x)) === normDeptName(dept));
			if (!d?.parquetKey || !d?.centroid) return false;
			keepViewOnce = !!opts.keepView;
			handleSelectFloodDpto(nameOf(d), d.parquetKey, d.centroid as [number, number]);
			return true;
		}
		// handleCensoDeptSelect picks the cells from centroidCache: wait for the layer's hexagons (60 s at most).
		const t0 = Date.now();
		while ((hexStore.loading || hexStore.centroidCache.size === 0) && Date.now() - t0 < 60000) {
			await new Promise((r) => setTimeout(r, 300));
		}
		if (lensStore.activeAnalysis?.id !== analysisId) return false;
		keepViewOnce = !!opts.keepView;
		// The list may not have the department: a later click of the user's must still fly.
		if (keepViewOnce) setTimeout(() => { keepViewOnce = false; }, 3000);
		hexStore.requestDeptPick(dept, hexStore.territoryPrefix, !!opts.keepMode);
		return true;
	}

	/** «ir» on a saved zone of another department: open it in the active layer and frame its zones. */
	async function goToZoneDept(key: string) {
		const i = key.indexOf('|');
		const prefix = key.slice(0, i), name = key.slice(i + 1);
		const a = lensStore.activeAnalysis;
		if (!a || !name) return;
		const tid = Object.values(TERRITORY_REGISTRY).find(t => t.parquetPrefix === prefix)?.id;
		if (!tid || a.coverage?.[tid] === 'unavailable') return;
		if (tid !== territoryStore.activeTerritory.id) {
			territoryStore.setTerritory(tid);
			hexStore.setTerritoryPrefix(prefix);
			await tick();
		}
		hexStore.flyToZonesKey = key;
		if (!(await openDeptByName(a.id, name))) hexStore.flyToZonesKey = null;
	}

	function handleSelectFloodDpto(dpto: string, parquetKey: string, centroid: [number, number]) {
		if (_deptDebounceTimer) clearTimeout(_deptDebounceTimer);
		_deptDebounceTimer = setTimeout(() => {
			_deptDebounceTimer = null;
			_doSelectFloodDpto(dpto, parquetKey, centroid);
		}, 80);
	}

	async function _doSelectFloodDpto(dpto: string, parquetKey: string, centroid: [number, number]) {
		const token = ++_deptToken;
		// Read now: syncZones may frame the zones (and clear the request) before the render below.
		const keepView = keepViewOnce || !!hexStore.flyToZonesKey;
		keepViewOnce = false;
		// Don't clear the map upfront — previous dept stays visible while new data loads.
		mapComponent?.clearHexZoneHighlight();
		mapStore.setActiveHexLayer(hexStore.activeLayer?.id ?? null);
		if (hexStore.activeLayer) {
			// The unit of what the map paints (the tab's column), not of the layer's configured primary:
			// economic_activity paints employment (%) while its primary is VIIRS (nW/cm²/sr).
			mapComponent?.setHexLayerInfo(i18n.t(hexStore.activeLayer.titleKey), hexStore.activeLayer.colorScale === 'categorical', getHexPrimaryUnit(hexStore.activeLayer, hexStore.effectivePrimary));
		}
		// Load dept data + compute color domain in parallel.
		await Promise.all([
			hexStore.loadDepartment(dpto, parquetKey),
			hexStore.ensureColorDomain().catch(() => {}),
		]);

		// If a newer dept was clicked while we were loading, abandon this render.
		if (_deptToken !== token) return;

		prevDataVersion = hexStore.dataVersion;
		// rAF: render at the next animation frame — no artificial delay, and drops stale frames.
		requestAnimationFrame(() => {
			if (_deptToken !== token) return;
			const allEntries = hexStore.choroplethEntries;
			// Only Misiones (empty prefix) needs PIP — all other territories pre-filtered in pipeline.
			const needsPip = hexStore.territoryPrefix === '';
			// Nodata gray fill (incl. the border buffer added in loadDepartment) always renders —
			// it covers the dark sliver where the generalized province boundary is inset from the
			// real border. Data cells stay province-bounded to prevent cross-province leakage.
			const entries = needsPip
				? allEntries.filter(e => {
					if (e.nodata) return true;
					const [lat, lng] = cellToLatLng(e.h3index);
					return isInsideActiveTerritory(lat, lng, hexStore.territoryPrefix);
				})
				: allEntries;
			if (entries.length > 0) {
				let colorScale: 'flood' | 'sequential' | 'diverging' | 'categorical' | 'green' | 'warm' | 'night' = hexStore.activeLayer?.colorScale ?? 'flood';
				if (hexStore.activeLayer?.temporal && hexStore.temporalMode === 'delta') colorScale = 'diverging';
				mapComponent?.setHexChoropleth(entries, colorScale, hexStore.colorDomain ?? undefined, colorClassesFor(colorScale));
				analysisDataLoaded = true;
				// Zones to frame (a saved zone's «ir», a link) or the camera kept: syncZones moves it.
				if (hexStore.deptBbox && !keepView) mapComponent?.fitBoundsDept(hexStore.deptBbox);
			} else if (hexStore.activeLayer?.macizos && hexStore.deptBbox) {
				mapComponent?.clearHexChoropleth();
				mapComponent?.fitBoundsDept(hexStore.deptBbox);
			} else {
				mapComponent?.clearHexChoropleth();
				mapComponent?.flyToCoords(centroid[1], centroid[0], 10);
			}
		});
	}

	function clearAll() {
		mapStore.clearRadios();
		mapStore.clearDistricts();
		mapStore.clearHexState();
		mapComponent?.setHexLayerInfo('', false);
		hexStore.clearAll();
		// A full reset (Escape, home) leaves the saved zones alone: only their ✕ or «Limpiar zonas»
		// removes them (author, 8-nov-2026). The next layer opens on its ranking.
		hexStore.workDept = null;
		mapComponent?.clearRadioHighlight();
		mapComponent?.setDistrictHighlight([]);
		mapComponent?.clearAnalysisChoropleth();
		mapComponent?.clearHexChoropleth();
		mapComponent?.clearHexZoneHighlight();
		lensStore.clearSelection();
		lensStore.clearDpto();
		lensStore.clearAnalysis();
		lassoStore.clearZones();
		mapComponent?.clearZoneHighlight();
		if (lassoStore.active) {
			lassoStore.toggle();
			mapComponent?.setLassoMode(false);
		}
		showAbout = true;
	}

	// "Spatia" = home, always the same place: the map as spatia.ar opens — no lens,
	// no layer, no selection, default territory in the trinational regional view,
	// the same NEA framing the regional effect applies on load, side panel closed,
	// clean URL.
	function goHome() {
		clearAll();
		showAbout = false;
		lensStore.reset();
		territoryStore.setTerritory(getDefaultTerritory().id);
		territoryStore.enterRegionalMode(null);
		territoryStore.homeView = true;
		// After the territory/regional effects (which may queue their own flyToBbox at 100–150 ms).
		setTimeout(() => mapComponent?.flyToBbox(HOME_BBOX), 300);
	}

</script>

<!-- Canonical for the map only: app.html is shared by every route, and a second canonical
     there would contradict the one each prerendered page sets through <Seo>. -->
<svelte:head>
	<link rel="canonical" href="https://spatia.ar/" />
</svelte:head>

<div class="flex h-screen w-full">
	<!-- Map + overlays -->
	<div class="flex-1 flex flex-col relative min-w-0">
		<!-- Header -->
		<div class="app-header flex items-center justify-between px-4 py-2.5 border-b border-border z-10 shrink-0"
			style="background: rgba(var(--c-bg-rgb), 0.88); backdrop-filter: blur(8px);">
			<div class="flex items-center gap-4">
				<h1 class="app-title text-[15px] font-bold text-white tracking-wide">
					<button class="app-home cursor-pointer hover:opacity-80 transition-opacity" onclick={goHome} title={i18n.t('header.home')}>
						{i18n.t('header.title')}
					</button>
				</h1>
				<button
					class="text-[12px] text-white/70 hover:text-white cursor-pointer transition-colors border border-white/20 rounded px-2 py-0.5"
					onclick={() => { showAbout = !showAbout; }}
					title={i18n.t('header.whatIsThis')}>
					{i18n.t('header.whatIsThis')}
				</button>
			</div>

			<!-- Lens selector (center) -->
			<LensSelector {lensStore} onPick={() => { showAbout = false; }} />

			<div class="flex items-center gap-0.5">
				{#each (['es', 'en', 'gn', 'pt'] as Locale[]) as lang}
					<button
						class="px-2.5 py-1 text-[11px] font-semibold rounded-full cursor-pointer border transition-all {i18n.locale === lang ? 'bg-white/10 text-white border-white/30' : 'bg-transparent text-white/50 border-transparent hover:text-white'}"
						onclick={() => i18n.setLocale(lang)}>
						{lang === 'pt' ? 'BR' : lang.toUpperCase()}
					</button>
				{/each}
			</div>
		</div>

		<!-- Map container -->
		<div bind:this={mapContainer} class="flex-1 relative min-h-0">
			<MapComponent bind:this={mapComponent} {mapStore} />
			{#if !bivariateMapOn && !lisaMapOn}<MapLegend {hexStore} />{/if}

			{#if hexStore.loading}
				<div class="loading-overlay">
					<div class="loading-spinner"></div>
					<span class="loading-text">{i18n.t('analysis.loading')}</span>
				</div>
			{/if}

			<!-- Regional/compare viewport too wide: neighbor-territory hexes load on zoom-in (B) -->
			{#if regionalViewportTooWide}
				<div class="absolute top-3 left-1/2 -translate-x-1/2 z-30 px-3 py-1.5 rounded-full bg-black/75 backdrop-blur-md border border-white/15 text-[11px] text-white/80 shadow-lg pointer-events-none">
					🔍 {i18n.t('map.hint.zoomNeighbours')}
				</div>
			{/if}

			{#if itapuaHovering && territoryStore.regionalMode}
				<div class="itapua-hover-badge">🇵🇾 Paraguay · datos satelitales comparables</div>
			{/if}

			<!-- Orientation controls (bottom-left): basemap Claro · Oscuro · Satélite (always:
			     it also sets the site's theme, so it is on the start view too) + hex opacity
			     slider (hexagon layers only — it's what the slider targets). -->
			<div class="absolute bottom-4 left-4 z-30 flex flex-col gap-1.5 px-2.5 py-2 rounded-lg bg-black/75 backdrop-blur-md border border-white/15 shadow-lg text-[11px] text-white/85">
				<div class="flex gap-1" role="group" aria-label={i18n.t('map.basemap.label')}>
					{#each FONDOS as f (f)}
						<button
							aria-pressed={themeStore.fondo === f}
							class="px-2 py-1 rounded transition-colors {themeStore.fondo === f ? 'bg-white/20 text-white' : 'text-white/60 hover:text-white'}"
							onclick={() => setFondo(f)}>
							{i18n.t(`map.basemap.${f}`)}
						</button>
					{/each}
				</div>
				{#if lensStore.activeAnalysis && hexStore.activeLayer}
					<label class="flex items-center gap-2">
						<span class="opacity-70">{i18n.t('map.opacity')}</span>
						<input type="range" min="0.3" max="0.9" step="0.05"
							bind:value={mapStore.hexOpacity}
							oninput={() => mapComponent?.applyHexOpacity()}
							class="w-24 accent-sky-400 cursor-pointer" />
					</label>
					{#if ['misiones', 'corrientes', 'chaco', 'formosa'].includes(territoryStore.activeTerritory?.id ?? '')}
						<label class="flex items-center gap-2 cursor-pointer">
							<input type="checkbox" bind:checked={mapStore.apVisible} class="accent-sky-400 cursor-pointer" />
							<span class="opacity-70">{i18n.t('map.ap.toggle')}</span>
						</label>
					{/if}
				{/if}
			</div>

			<!-- LOD: giant departments render an aggregated overview; values shown are spatial means -->
			{#if hexStore.selectedHexResLevel !== null && hexStore.selectedDpto}
				<div class="lod-hint absolute bottom-28 left-4 z-30 px-3 py-1.5 rounded-full bg-black/75 backdrop-blur-md border border-amber-400/30 text-[11px] text-amber-200/90 shadow-lg pointer-events-none">
					🔬 {i18n.t(hexStore.effectiveColorClasses?.nominal ? 'map.hint.aggregatedMode' : 'map.hint.aggregated')}
				</div>
			{/if}

			<!-- Dept picker panel: shown when a perDept analysis is active and no dept selected yet -->
			{#if hexStore.activeLayer?.perDepartment && !hexStore.selectedDpto && !hexStore.loading}
				<DeptPickerPanel
					analysisId={hexStore.activeLayer.id}
					territoryPrefix={hexStore.territoryPrefix}
					onSelect={(name, parquetKey) => {
						const t = TERRITORY_REGISTRY[territoryStore.activeTerritory.id];
						loadDeptSummary(hexStore.activeLayer!.id, hexStore.territoryPrefix).then(summary => {
							const depts = summary?.departments as any[] | undefined;
							if (!depts?.length) return;
							const entry = depts.find(d => normDeptName(d.dpto ?? d.distrito ?? d.municipio) === normDeptName(name));
							if (!entry?.centroid) { handleSelectFloodDpto(name, parquetKey, [0, 0]); return; }
							handleSelectFloodDpto(name, parquetKey, entry.centroid as [number, number]);
						});
					}}
				/>
			{/if}

			<!-- Controls (positioned relative to map) -->
			<Controls
				{mapStore}
				hasSelection={mapStore.selectedRadios.size > 0}
				onClear={clearAll}
				lassoActive={lassoStore.active}
				onToggleLasso={handleToggleLasso}
				hasZones={lassoStore.zones.length > 0 || hexStore.hexZones.length > 0}
				onClearZones={() => { handleClearZones(); handleClearHexZones(); }}
			/>

			<!-- Sidebar (positioned relative to map) -->
			<Sidebar
				{mapStore}
				{lensStore}
				{lassoStore}
				{hexStore}
				{territoryStore}
				{showAbout}
				onRemoveRadio={handleRemoveRadio}
				onClearRadios={handleClearRadios}
				onSelectAnalysis={handleSelectAnalysis}
				onRemoveZone={handleRemoveZone}
				onClearZones={handleClearZones}
				onRemoveHexZone={handleRemoveHexZone}
				onClearHexZones={handleClearHexZones}
				onGoToZoneDept={goToZoneDept}
				onClearSavedZones={(k) => { hexStore.clearSavedZones(k); refreshHighlights(); }}
				onHexZoneRenamed={refreshHighlights}
				onSelectFloodDpto={handleSelectFloodDpto}
			onDownloadRadioCsv={downloadRadioCsv}
			onDownloadRadioGeoJson={downloadRadioGeoJson}
			onDownloadRadiosSummary={downloadRadiosSummary}
			onShowLisa={handleShowLisa}
			onMoranBrush={handleMoranBrush}
			onHistogramBrush={handleHistogramBrush}
			onBivariateBrush={handleBivariateBrush}
			onShowBivariateMap={handleShowBivariateMap}
			onFlowBrush={handleFlowBrush}
			onSaveChartSelection={handleSaveChartSelection}
			onClearChartSelection={handleClearChartSelection}
			onRadioCensusBrush={handleRadioCensusBrush}
			onCensoDeptSelect={handleCensoDeptSelect}
			onRemoveDistrict={handleRemoveDistrict}
			onClearDistricts={handleClearDistricts}
			onDownloadDistrictGeoJson={downloadDistrictGeoJson}
			/>
		</div>
	</div>
</div>

{#if duckdbFailed || hexStore.loadError}
	<div style="position:fixed;bottom:24px;right:24px;z-index:9999;display:flex;align-items:flex-start;gap:12px;border-radius:10px;border:1px solid rgba(239,68,68,0.35);background:rgba(var(--c-bg-rgb), 0.92);padding:12px 16px;backdrop-filter:blur(8px);max-width:280px;box-shadow:0 4px 24px rgba(0,0,0,0.6)">
		<span style="font-size:12px;color:var(--c-t-fca5a5);line-height:1.5;flex:1">
			{duckdbFailed ? i18n.t('error.engineFailed') : i18n.t('error.dataLoadFailed')}
		</span>
		<button
			style="background:none;border:none;cursor:pointer;color:rgba(252,165,165,0.5);font-size:16px;line-height:1;padding:0;flex-shrink:0"
			onclick={() => { duckdbFailed = false; hexStore.clearLoadError(); }}
		>✕</button>
	</div>
{/if}

<style>
	.territory-col {
		width: 145px;
		flex-shrink: 0;
		background: rgba(var(--c-bg-rgb), 0.88);
		backdrop-filter: blur(8px);
		border-right: 1px solid rgba(var(--c-fg-rgb), 0.07);
		overflow-y: auto;
		padding: 10px 6px 16px;
		scrollbar-width: thin;
		scrollbar-color: var(--c-line-2) transparent;
	}
	@media (max-width: 768px) {
		.territory-col { display: none; }
	}

	.loading-overlay {
		position: absolute;
		top: 0;
		left: 0;
		right: 0;
		bottom: 0;
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 10px;
		background: rgba(var(--c-bg-rgb), 0.5);
		z-index: 8;
		pointer-events: none;
	}
	.loading-spinner {
		width: 18px;
		height: 18px;
		border: 2px solid rgba(var(--c-fg-rgb), 0.15);
		border-top-color: var(--c-accent);
		border-radius: 50%;
		animation: spin 0.7s linear infinite;
	}
	.loading-text {
		font-size: 12px;
		color: rgba(var(--c-fg-rgb), 0.7);
		font-weight: 500;
	}
	@keyframes spin {
		to { transform: rotate(360deg); }
	}

	@media (max-width: 768px) {
		.app-header {
			padding: 6px 8px;
			flex-wrap: wrap;
			gap: 4px;
		}
		.app-title {
			font-size: 13px;
		}
	}
	.app-home {
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		color: inherit;
		letter-spacing: inherit;
	}

	.itapua-hover-badge {
		position: absolute;
		bottom: 44px;
		left: 16px;
		transform: none;
		background: rgba(var(--c-bg-rgb), 0.88);
		border: 1px solid rgba(139, 92, 246, 0.45);
		color: var(--c-t-a78bfa);
		font-size: 10px;
		font-weight: 600;
		padding: 4px 12px;
		border-radius: 20px;
		pointer-events: none;
		z-index: 10;
		backdrop-filter: blur(6px);
		white-space: nowrap;
		letter-spacing: 0.03em;
	}
	/* On paper the amber hint read faint: ink, like the legend's scale labels. */
	:global(:root[data-theme='claro']) .lod-hint { color: var(--c-ink-1); border-color: var(--c-line-3); }
</style>
