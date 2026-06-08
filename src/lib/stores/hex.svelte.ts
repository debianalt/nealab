import { query, isReady } from '$lib/stores/duckdb';
import { PARQUETS, HEX_LAYER_REGISTRY, getFloodDptoUrl, getScoresDptoUrl, getSatDptoUrl, getSatGlobalUrl, getEudrParquetUrl, getTemporalCol, type HexLayerConfig, type HexVariable, type TemporalMode } from '$lib/config';
import { pointInPolygon } from '$lib/utils/geometry';
import { findDeptFeature } from '$lib/utils/deptBoundaries';
import { loadDeptSummary } from '$lib/utils/deptSummaries';
import { i18n } from '$lib/stores/i18n.svelte';
import { cellToLatLng, cellToBoundary, polygonToCells, cellToParent } from 'h3-js';

// ── LOD (level of detail) for giant departments ──────────────────────────────
// Chaco/Boqueron districts reach 400K-750K res-9 hexes. Building one polygon per
// res-9 cell on the main thread (cellToBoundary loop + setData clone) freezes the
// UI for seconds. Above LOD_ACTIVATION_ROWS we aggregate to a coarser H3 resolution
// chosen so the rendered polygon count lands near LOD_TARGET_POLYGONS — visually
// near-identical at department-overview zoom, but ~10-50x fewer polygons.
const LOD_ACTIVATION_ROWS = 60000;
const LOD_TARGET_POLYGONS = 15000;
// Returns the FINEST coarse resolution (8..4) whose estimated parent count is within
// the target, or null when no aggregation is needed (department renders fine at res-9).
function pickCoarseRes(n: number): number | null {
	if (n <= LOD_ACTIVATION_ROWS) return null;
	for (let r = 8; r >= 4; r--) {
		// each step up the hierarchy holds ~7 children
		if (n / Math.pow(7, 9 - r) <= LOD_TARGET_POLYGONS) return r;
	}
	return 4;
}
// Rough res-9 cell count from a polygon's bbox area (res-9 hex ≈ 0.105 km²). Overestimates
// (bbox ⊇ polygon) so it errs toward skipping the edge-fill — fine, the fill is cosmetic.
// h3-js v4 dropped maxPolygonToCellsSize, so estimate instead of enumerating.
function estimateRes9Cells(ring: number[][]): number {
	let minLng = Infinity, maxLng = -Infinity, minLat = Infinity, maxLat = -Infinity;
	for (const [lng, lat] of ring) {
		if (lng < minLng) minLng = lng; if (lng > maxLng) maxLng = lng;
		if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat;
	}
	if (!Number.isFinite(minLng)) return 0;
	const midLat = ((minLat + maxLat) / 2) * Math.PI / 180;
	const wKm = (maxLng - minLng) * 111 * Math.cos(midLat);
	const hKm = (maxLat - minLat) * 111;
	return Math.abs(wKm * hKm) / 0.105;
}

const ZONE_COLORS = ['#60a5fa', '#f97316', '#22c55e', '#a855f7', '#ef4444', '#eab308'];
const ZONE_LABELS = ['A', 'B', 'C', 'D', 'E', 'F'];

// Persistent cache that survives clearAll() / layer toggling
interface LayerCache {
	data: Map<string, Record<string, any>>;
	centroids: Map<string, [number, number]>;
	boundaries: Map<string, number[][]>;
	provincialAvg: number[] | null;
}
const layerDataCache = new Map<string, LayerCache>();

// Per-department cache: keyed by `layerId:parquetKey:territoryPrefix`.
// Avoids re-fetching + re-computing H3 geometry on repeated dept visits.
interface DeptCache {
	data: Map<string, Record<string, any>>;
	centroids: Map<string, [number, number]>;
	boundaries: Map<string, number[][]>;
	bbox: [number, number, number, number] | null;
	resLevel?: number | null; // coarse H3 res if rendered with LOD aggregation, else undefined/null
}
const deptDataCache = new Map<string, DeptCache>();

// Schema (column-name set) cache keyed by parquet URL. The URL carries the
// `?v=N` cache-buster, so a schema change (version bump) invalidates it
// naturally. Avoids a redundant `SELECT * LIMIT 0` round-trip on every
// global/viewport load (schema is invariant for a given URL).
const schemaColsCache = new Map<string, Set<string>>();
async function getParquetCols(url: string): Promise<Set<string>> {
	const cached = schemaColsCache.get(url);
	if (cached) return cached;
	const schemaRes = await query(`SELECT * FROM '${url}' LIMIT 0`);
	const cols = new Set<string>(schemaRes.schema.fields.map((f: any) => f.name as string));
	schemaColsCache.set(url, cols);
	return cols;
}

// Cap on cached depts. Persists across territory switches (keys include the prefix), so
// evict the oldest insertion once over the cap to keep memory bounded after many switches.
const DEPT_CACHE_MAX = 40;
function setDeptCache(key: string, value: DeptCache): void {
	if (deptDataCache.has(key)) deptDataCache.delete(key); // refresh recency on re-set
	deptDataCache.set(key, value);
	if (deptDataCache.size > DEPT_CACHE_MAX) {
		const oldest = deptDataCache.keys().next().value;
		if (oldest !== undefined) deptDataCache.delete(oldest);
	}
}

export interface HexSelectionData {
	color: string;
	data: Record<string, any>;
}

export interface HexZoneStats {
	hexCount: number;
	rawValues: number[];
	normalizedValues: number[];
}

export interface HexZone {
	id: string;
	color: string;
	h3indices: string[];
	polygon: [number, number][];
	stats: HexZoneStats;
}

const NON_NUMERIC_COLS = new Set(['type', 'type_label', 'pca_1', 'pca_2', 'pca_3', 'score', 'flood_risk_score', 'risk_score', 'territorial_type']);

export class HexStore {
	activeLayer: HexLayerConfig | null = $state(null);
	visibleData: Map<string, Record<string, any>> = $state(new Map());
	selectedHexes: Map<string, HexSelectionData> = $state(new Map());
	hexZones: HexZone[] = $state([]);
	loading: boolean = $state(false);
	loadError: string | null = $state(null);
	temporalMode: TemporalMode = $state('current');

	private colorIndex = 0;
	private provincialAvg: number[] | null = $state(null);
	colorDomain: [number, number] | null = $state(null);
	selectedDpto: string | null = $state(null);
	selectedParquetKey: string | null = $state(null);
	// When the selected department is currently rendered with LOD aggregation, this holds
	// the coarse H3 resolution used (e.g. 7); null = full res-9 detail. Lets the UI tell the
	// user the hexes are a spatial aggregate. Flips to null when zoom-in loads res-9 detail.
	selectedHexResLevel: number | null = $state(null);
	// True while a LOD-capable (giant) department is selected, independent of the current
	// zoom/res. Gates the viewport moveend effect that swaps coarse↔res-9 detail.
	lodDeptActive: boolean = $state(false);
	// Context needed to re-query res-9 detail for the viewport on zoom-in (and to restore
	// the cached coarse overview on zoom-out). Null when no LOD dept is active.
	private lodDeptContext: { url: string; coarseRes: number; cacheKey: string } | null = null;
	private _prefetchToken = 0;

	// ── Compare dept (cross-territory dept-to-dept comparison) ──────────────
	compareVisibleData: Map<string, Record<string, any>> = $state(new Map());
	private compareBoundaryCache: Map<string, number[][]> = new Map();
	compareDpto: string | null = $state(null);
	compareTerritoryPrefix: string | null = $state(null);
	compareDataVersion: number = $state(0);
	// When a full-territory global is WANTED for the compare slot (vs a single dept),
	// this holds its prefix. Rows are loaded per-viewport (B) by loadGlobalViewport,
	// driven by the moveend effect in +page. Null = no full-global compare wanted.
	compareGlobalPrefix: string | null = $state(null);

	// ── Regional mode (3rd territory hex slot) ───────────────────────────
	regionalVisibleData: Map<string, Record<string, any>> = $state(new Map());
	private regionalBoundaryCache: Map<string, number[][]> = new Map();
	regionalDataVersion: number = $state(0);
	// Same as compareGlobalPrefix, for the regional (3rd territory) slot.
	regionalGlobalPrefix: string | null = $state(null);

	// Bounding boxes for dept highlight outlines on map
	deptBbox: [number, number, number, number] | null = $state(null);
	compareDeptBbox: [number, number, number, number] | null = $state(null);

	get numericVariables(): HexVariable[] {
		return this.activeLayer?.variables.filter(v => !NON_NUMERIC_COLS.has(v.col)) ?? [];
	}

	// Monotonic counter: increments on every meaningful visibleData change.
	// Used by $effect to detect changes regardless of data size.
	dataVersion: number = $state(0);

	// Memoization caches — avoids O(n) recompute on every call for all three data slots.
	private _entriesVersion = -1;
	private _entriesCache: ReturnType<HexStore['choroplethEntries_compute']> = [];
	private _compareEntriesVersion = -1;
	private _compareEntriesCache: ReturnType<HexStore['compareChoroplethEntries_compute']> = [];
	private _regionalEntriesVersion = -1;
	private _regionalEntriesCache: ReturnType<HexStore['regionalChoroplethEntries_compute']> = [];

	// Pre-computed geometry caches (built once at load, reused everywhere)
	centroidCache: Map<string, [number, number]> = new Map(); // h3index → [lng, lat]
	boundaryCache: Map<string, number[][]> = new Map(); // h3index → [[lng, lat], ...]

	setTemporalMode(mode: TemporalMode) {
		this.temporalMode = mode;
		this.dataVersion++;
	}

	setLayer(layerId: string | null) {
		this._globalLoadState.clear(); // layer changed → allow regional/compare reloads for the new layer
		if (!layerId) {
			this.activeLayer = null;
			this.visibleData = new Map();
			this.centroidCache = new Map();
			this.boundaryCache = new Map();
			this.selectedDpto = null;
			this.selectedParquetKey = null;
			this.selectedHexResLevel = null;
			this.lodDeptActive = false;
			this.lodDeptContext = null;
			this.deptBbox = null;
			this.temporalMode = 'current';
			this.dataVersion++;
			this.clearSelection();
			this.clearHexZones();
			return;
		}
		const cfg = HEX_LAYER_REGISTRY[layerId];
		if (!cfg) return;
		this.activeLayer = cfg;
		this.temporalMode = 'current';
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.selectedHexResLevel = null;
		this.lodDeptActive = false;
		this.lodDeptContext = null;
		this.deptBbox = null;
		this.clearCompareDept();
		this.clearRegionalData();

		// EUDR: global dataset over many provinces — too heavy to load whole.
		// Load on demand by viewport (see loadEudrViewport). Start empty.
		if (cfg.id === 'eudr') {
			this.visibleData = new Map();
			this.boundaryCache = new Map();
			this.centroidCache = new Map();
			this.dataVersion++;
			this.loading = false;
			return;
		}

		// Per-department layers: don't load all data, wait for department selection.
		// Background-fetch all dept parquets so the first click is fast.
		if (cfg.perDepartment) {
			this.loading = false;
			this._prefetchDeptParquets(cfg);
			return;
		}

		// Restore from persistent cache if available (instant re-activation)
		const cached = layerDataCache.get(layerId);
		if (cached) {
			this.visibleData = cached.data;
			this.centroidCache = cached.centroids;
			this.boundaryCache = cached.boundaries;
			this.provincialAvg = cached.provincialAvg;
			this.dataVersion++;
			this.loading = false;
			return;
		}

		this.provincialAvg = null;
		this.loadVisibleData();
	}

	territoryPrefix: string = $state('');

	setTerritoryPrefix(prefix: string) {
		if (this.territoryPrefix === prefix) return;
		this.territoryPrefix = prefix;
		layerDataCache.clear(); // keyed by layerId only → must clear to avoid serving the
		                        // previous province's data under the same layer id.
		// deptDataCache is NOT cleared: its keys include the territoryPrefix, so entries from
		// other provinces never collide. Keeping them makes switching BACK to a visited province
		// (and re-clicking its depts) instant. Growth is bounded by setDeptCache()'s LRU cap.
		this._globalLoadState.clear(); // territory changed → allow regional/compare reloads
		this._prefetchToken++;
		this.colorDomain = null;
		this.visibleData = new Map();
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.selectedHexResLevel = null;
		this.lodDeptActive = false;
		this.lodDeptContext = null;
		this.deptBbox = null;
		this.dataVersion++;
		this.clearCompareDept();
	}

	/** Territory-aware URL for the global parquet of a layer. */
	private layerGlobalUrl(layer: HexLayerConfig): string | undefined {
		const url = PARQUETS[layer.parquet as keyof typeof PARQUETS];
		if (!url) return undefined;
		// Only sat_* parquets are territory-specific; EUDR, emsa, etc. use a fixed global URL
		if (!this.territoryPrefix || !layer.parquet?.startsWith('sat_')) return url;
		return url.replace('/data/', `/data/${this.territoryPrefix}`);
	}

	/** Per-department parquet URL for a layer (mirrors the dispatch in loadDepartment). */
	private deptUrlFor(layer: HexLayerConfig, parquetKey: string): string {
		if (layer.id === 'flood_risk') return getFloodDptoUrl(parquetKey, this.territoryPrefix);
		if (layer.parquet?.startsWith('sat_')) return getSatDptoUrl(layer.id, parquetKey, this.territoryPrefix);
		return getScoresDptoUrl(parquetKey, this.territoryPrefix);
	}

	/** Columns the dept UI actually consumes — used to project instead of SELECT *. */
	private async deptWantedCols(layer: HexLayerConfig, url: string): Promise<string[]> {
		const actualCols = await getParquetCols(url);
		const wanted = ['h3index', layer.primaryVariable, 'type', 'type_label',
			...layer.variables.flatMap(v => [v.col, v.rawCol]),
			...(layer.petalVars?.map(v => v.col) ?? [])];
		if (layer.temporal) {
			for (const v of [...layer.variables.map(v => v.col), layer.primaryVariable]) {
				wanted.push(getTemporalCol(v, 'baseline'), getTemporalCol(v, 'delta'));
			}
		}
		return wanted.filter((c): c is string => !!c && actualCols.has(c)).filter((c, i, a) => a.indexOf(c) === i);
	}

	async loadDepartment(dpto: string, parquetKey: string) {
		if (!this.activeLayer) return;
		const layer = this.activeLayer;

		// Abort any in-flight background precompute: the user clicked a specific dept and
		// its query must NOT queue behind ~17 sequential SELECT* precomputes on the single
		// DuckDB-WASM thread. This is the dominant cause of carbon feeling "broken".
		this._prefetchToken++;

		this.loading = true;
		this.loadError = null;
		this.selectedDpto = dpto;
		this.selectedParquetKey = parquetKey;
		this.visibleData = new Map();
		this.clearSelection();
		this.clearHexZones();
		this.clearCompareDept();
		this.clearRegionalData();
		// Note: dataVersion is NOT incremented here — we only increment after data is ready.
		// This prevents a blank-map flash that would occur if $effects fire on the empty visibleData.

		// Reset LOD context for the new selection (set below if this dept is giant).
		this.lodDeptActive = false;
		this.lodDeptContext = null;
		const url = this.deptUrlFor(layer, parquetKey);

		try {
			// Fast path: return cached geometry + data without a DuckDB round-trip.
			const cacheKey = `${layer.id}:${parquetKey}:${this.territoryPrefix}`;
			const deptCached = deptDataCache.get(cacheKey);
			if (deptCached) {
				this.visibleData = deptCached.data;
				this.centroidCache = deptCached.centroids;
				this.boundaryCache = deptCached.boundaries;
				this.deptBbox = deptCached.bbox;
				this.selectedHexResLevel = deptCached.resLevel ?? null;
				if (deptCached.resLevel != null) {
					// Cached coarse overview → re-arm viewport detail (zoom-in re-queries res-9).
					this.lodDeptActive = true;
					this.lodDeptContext = { url, coarseRes: deptCached.resLevel, cacheKey };
				}
				this.dataVersion++;
				this.loading = false;
				// provincialAvg deferred — loaded lazily by petal/zone paths (see note below).
				return;
			}

			// Decide LOD from a cheap row count (parquet footer read) BEFORE pulling any
			// column data. Carbon dept parquets are single-row-group 34 MB; at LOD overview
			// we only render the choropleth, so we project to JUST the primary column and skip
			// downloading the ~20 unused variable columns (34 MB → ~4-5 MB). The full variable
			// set arrives lazily with the res-9 viewport detail on zoom-in.
			const countRes = await query(`SELECT count(*) AS n FROM '${url}'`);
			const totalRows = Number((countRes.get(0)!.toJSON() as Record<string, any>).n) || 0;
			const coarseRes = pickCoarseRes(totalRows);
			this.selectedHexResLevel = coarseRes;

			let cols: string[];
			if (coarseRes !== null) {
				const actualCols = await getParquetCols(url);
				const minimal = ['h3index', layer.primaryVariable, 'type_label'];
				if (layer.temporal) minimal.push(getTemporalCol(layer.primaryVariable, 'baseline'), getTemporalCol(layer.primaryVariable, 'delta'));
				cols = minimal.filter((c): c is string => !!c && actualCols.has(c)).filter((c, i, a) => a.indexOf(c) === i);
			} else {
				// Small dept → full projection (choropleth + panel + petal need the variable set).
				cols = await this.deptWantedCols(layer, url);
			}
			const result = await query(`SELECT ${cols.map(c => `"${c}"`).join(', ')} FROM '${url}'`);

			const data = new Map<string, Record<string, any>>();
			const centroids = new Map<string, [number, number]>();
			const boundaries = new Map<string, number[][]>();

			const resultCols = result.schema.fields
				.map((f: any) => f.name)
				.filter((name: string) => name !== 'h3index');
			const h3indexVec = result.getChild('h3index');
			const colVecs = Object.fromEntries(
				resultCols.map((col: string) => [col, result.getChild(col)])
			);

			// LOD branch: aggregate giant departments to a coarser resolution so we never build
			// hundreds of thousands of polygons on the main thread (the panel-freeze). coarseRes
			// + selectedHexResLevel were already set above from the cheap row count.
			if (coarseRes !== null) {
				// ── Aggregate res-9 → coarseRes ─────────────────────────────────────────
				// Per-row work is ONLY cellToParent (bitwise, cheap) + numeric accumulation;
				// cellToBoundary (the expensive geometry call) runs once per coarse parent,
				// not once per res-9 cell → ~10-50x fewer geometry builds + smaller setData clone.
				// Average every column except the true string-categoricals — this guarantees the
				// primaryVariable is aggregated even if a layer ever uses a score-named column
				// (NON_NUMERIC_COLS lists 'score', which would otherwise drop it → blank cells).
				const LOD_CATEGORICAL = new Set(['type', 'type_label', 'territorial_type']);
				const numericCols = resultCols.filter((c: string) => !LOD_CATEGORICAL.has(c));
				const labelVec = resultCols.includes('type_label') ? result.getChild('type_label') : null;
				const sums = new Map<string, Float64Array>();
				const counts = new Map<string, Int32Array>();
				const labels = new Map<string, Map<string, number>>();
				for (let i = 0; i < result.numRows; i++) {
					let parent: string;
					try { parent = cellToParent(String(h3indexVec!.get(i)), coarseRes); } catch { continue; }
					let s = sums.get(parent);
					let c = counts.get(parent);
					if (!s || !c) { s = new Float64Array(numericCols.length); c = new Int32Array(numericCols.length); sums.set(parent, s); counts.set(parent, c); }
					for (let k = 0; k < numericCols.length; k++) {
						const v = colVecs[numericCols[k]]?.get(i);
						if (v === null || v === undefined) continue;
						const num = Number(v);
						if (!Number.isFinite(num)) continue;
						s[k] += num; c[k]++;
					}
					if (labelVec) {
						const lv = labelVec.get(i);
						if (lv !== null && lv !== undefined) {
							const key = String(lv);
							let lm = labels.get(parent);
							if (!lm) { lm = new Map(); labels.set(parent, lm); }
							lm.set(key, (lm.get(key) ?? 0) + 1);
						}
					}
				}
				for (const [parent, s] of sums) {
					const c = counts.get(parent)!;
					const values: Record<string, any> = {};
					for (let k = 0; k < numericCols.length; k++) {
						if (c[k] > 0) values[numericCols[k]] = s[k] / c[k]; // spatial mean — display only
					}
					const lm = labels.get(parent);
					if (lm) { // modal categorical label
						let best = '', bestN = -1;
						for (const [k, n] of lm) { if (n > bestN) { bestN = n; best = k; } }
						values['type_label'] = best;
					}
					data.set(parent, values);
					try {
						const [lat, lng] = cellToLatLng(parent);
						centroids.set(parent, [lng, lat]);
						const boundary = cellToBoundary(parent);
						const coords = boundary.map(([la, lo]: [number, number]) => [lo, la]);
						coords.push(coords[0]);
						boundaries.set(parent, coords);
					} catch { /* skip invalid h3 */ }
				}
				// Edge-fill (res-9 polygonToCells over the dept polygon) is intentionally skipped
				// in LOD mode: pointless at coarse res and ruinous for giant AR/BR polygons
				// (enumerates 100K+ res-9 cells on the main thread → the freeze we're avoiding).
				setDeptCache(cacheKey, { data, centroids, boundaries, bbox: HexStore.bboxFromCentroids(centroids), resLevel: coarseRes });
				this.visibleData = data;
				this.centroidCache = centroids;
				this.boundaryCache = boundaries;
				this.deptBbox = HexStore.bboxFromCentroids(centroids);
				// Arm viewport detail: zoom-in re-queries res-9 cells in view (loadDeptViewport).
				this.lodDeptActive = true;
				this.lodDeptContext = { url, coarseRes, cacheKey };
				this.dataVersion++;
				this.loading = false;
				return;
			}

			for (let i = 0; i < result.numRows; i++) {
				const h3index = String(h3indexVec!.get(i));
				try {
					const [lat, lng] = cellToLatLng(h3index);
					// Per-dpto parquets already contain only hexes for the selected dept
					// (assigned via h3_admin_crosswalk). The simplified province polygons
					// (corrientes_boundary.json ~210 vertices) miss irregular borders like
					// the Paraná river edge, producing false "straight lines" of missing
					// hexes at the dept border. No additional filter needed here.
					const values: Record<string, any> = {};
					for (const col of resultCols) {
						const val = colVecs[col]?.get(i);
						if (val === null || val === undefined) continue;
						const num = Number(val);
						values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
					}
					data.set(h3index, values);
					centroids.set(h3index, [lng, lat]);
					const boundary = cellToBoundary(h3index);
					const coords = boundary.map(([lat, lng]) => [lng, lat]);
					coords.push(coords[0]);
					boundaries.set(h3index, coords);
				} catch { /* skip invalid h3 */ }
			}

			// Fill missing hexes: the crosswalk used by split_by_admin.py can leave hexes
			// that geographically belong to this dept assigned to neighboring depts (border
			// polygon overlap/gap artifacts), AND the GEE raster export bbox may not cover
			// the full polygon extent. Result: visible "holes" inside the dept polygon.
			// Fix: enumerate all H3 res-9 cells inside the real dept polygon and add any
			// that are missing from the parquet as nodata hexes (rendered "Sin cobertura").
			// Only applicable to AR territories where we have polygon data.
			const deptFeature = findDeptFeature(dpto, this.territoryPrefix);
			if (deptFeature?.geometry) {
				const geom = deptFeature.geometry;
				const polygons: number[][][][] = geom.type === 'MultiPolygon'
					? geom.coordinates
					: geom.type === 'Polygon'
					? [geom.coordinates]
					: [];
				for (const poly of polygons) {
					try {
						// Guard giant polygons: a physically large dept (huge but sparse, so its
						// parquet stayed under the LOD row gate) would still enumerate hundreds of
						// thousands of res-9 cells here and freeze. Estimate from the bbox and skip
						// the edge-fill when it's too large (the fill is cosmetic hole-patching).
						if (estimateRes9Cells(poly[0] as number[][]) > LOD_ACTIVATION_ROWS) continue;
						// h3-js polygonToCells: coords as [lng, lat] (GeoJSON), third arg = isGeoJson
						const expected = polygonToCells(poly as any, 9, true);
						for (const h3index of expected) {
							if (data.has(h3index)) continue;
							try {
								const [lat, lng] = cellToLatLng(h3index);
								data.set(h3index, {});
								centroids.set(h3index, [lng, lat]);
								const boundary = cellToBoundary(h3index);
								const coords = boundary.map(([lat, lng]) => [lng, lat]);
								coords.push(coords[0]);
								boundaries.set(h3index, coords);
							} catch { /* skip invalid h3 */ }
						}
					} catch (e) {
						console.warn('[loadDept polygonToCells failed]', dpto, e);
					}
				}
			}

			// Persist computed geometry so re-visiting this dept is instant.
			setDeptCache(cacheKey, { data, centroids, boundaries, bbox: HexStore.bboxFromCentroids(centroids) });

			this.visibleData = data;
			this.centroidCache = centroids;
			this.boundaryCache = boundaries;
			this.deptBbox = HexStore.bboxFromCentroids(centroids);
			this.dataVersion++;

			// NOTE: provincialAvg (a heavy AVG query over the GLOBAL parquet) is intentionally
			// NOT loaded here. It is only needed to normalize the petal/zones, which load it
			// lazily on hex-click (+page hex-select) and in createHexZone. Keeping it off the
			// dept-load path removes a second global-parquet scan from the critical render.
		} catch (e) {
			console.warn('[loadDept FAIL]', dpto, e);
			this.loadError = 'dataLoadFailed';
		}

		this.loading = false;
	}

	// Viewport detail for a giant (LOD) department: re-query the res-9 cells currently in
	// view so zooming in shows real per-hex detail. cells=[] (zoomed out past the gate)
	// restores the cached coarse overview without a re-query. Mirrors the regional/compare
	// viewport pattern. Returns true when visibleData changed (caller re-renders).
	async loadDeptViewport(cells: string[]): Promise<boolean> {
		const ctx = this.lodDeptContext;
		const layer = this.activeLayer;
		if (!ctx || !layer || !isReady()) return false;

		// Zoomed out / too wide → restore the coarse overview (no re-query).
		if (cells.length === 0) {
			if (this.selectedHexResLevel === ctx.coarseRes) return false; // already coarse
			const cached = deptDataCache.get(ctx.cacheKey);
			if (!cached) return false;
			this.visibleData = cached.data;
			this.centroidCache = cached.centroids;
			this.boundaryCache = cached.boundaries;
			this.selectedHexResLevel = ctx.coarseRes;
			this.dataVersion++;
			return true;
		}

		// Load res-9 detail for the visible cells.
		try {
			const cols = await this.deptWantedCols(layer, ctx.url);
			const inList = cells.map(c => `'${c}'`).join(',');
			const result = await query(`SELECT ${cols.map(c => `"${c}"`).join(', ')} FROM '${ctx.url}' WHERE h3index IN (${inList})`);
			// Guard against a stale write: if a different dept/layer was selected while this
			// query was in flight, lodDeptContext changed — drop this result.
			if (this.lodDeptContext !== ctx) return false;
			const data = new Map<string, Record<string, any>>();
			const centroids = new Map<string, [number, number]>();
			const boundaries = new Map<string, number[][]>();
			const resultCols = result.schema.fields.map((f: any) => f.name).filter((n: string) => n !== 'h3index');
			const h3Vec = result.getChild('h3index');
			const colVecs = Object.fromEntries(resultCols.map((c: string) => [c, result.getChild(c)]));
			for (let i = 0; i < result.numRows; i++) {
				const h3index = String(h3Vec!.get(i));
				const values: Record<string, any> = {};
				for (const col of resultCols) {
					const val = colVecs[col]?.get(i);
					if (val === null || val === undefined) continue;
					const num = Number(val);
					values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
				}
				data.set(h3index, values);
				try {
					const [lat, lng] = cellToLatLng(h3index);
					centroids.set(h3index, [lng, lat]);
					const boundary = cellToBoundary(h3index);
					const coords = boundary.map(([la, lo]: [number, number]) => [lo, la]);
					coords.push(coords[0]);
					boundaries.set(h3index, coords);
				} catch { /* skip invalid h3 */ }
			}
			this.visibleData = data;
			this.centroidCache = centroids;
			this.boundaryCache = boundaries;
			this.selectedHexResLevel = null; // real res-9 detail now → no "aggregated" badge
			this.dataVersion++;
			return true;
		} catch (e) {
			console.warn('[loadDeptViewport]', e);
			return false;
		}
	}

	async loadCompareDept(dpto: string, parquetKey: string, comparePrefix: string): Promise<void> {
		if (!this.activeLayer) return;
		const layer = this.activeLayer;
		this.loadError = null;
		this.compareGlobalPrefix = null; // dept-scoped compare → stop the viewport global refresh

		let url: string;
		if (layer.id === 'flood_risk') {
			url = getFloodDptoUrl(parquetKey, comparePrefix);
		} else if (layer.parquet?.startsWith('sat_')) {
			url = getSatDptoUrl(layer.id, parquetKey, comparePrefix);
		} else {
			url = getScoresDptoUrl(parquetKey, comparePrefix);
		}

		// Fast path: reuse cached geometry.
		const compareCacheKey = `${layer.id}:${parquetKey}:${comparePrefix}`;
		const compareCached = deptDataCache.get(compareCacheKey);
		if (compareCached) {
			this.compareVisibleData = compareCached.data;
			this.compareBoundaryCache = compareCached.boundaries;
			this.compareDpto = dpto;
			this.compareTerritoryPrefix = comparePrefix;
			this.compareDeptBbox = compareCached.bbox ?? HexStore.bboxFromBounds(compareCached.boundaries);
			this.compareDataVersion++;
			return;
		}

		try {
			const result = await query(`SELECT * FROM '${url}'`);
			const data = new Map<string, Record<string, any>>();
			const bounds = new Map<string, number[][]>();

			const resultCols = result.schema.fields
				.map((f: any) => f.name)
				.filter((n: string) => n !== 'h3index');
			const h3Vec = result.getChild('h3index');
			const colVecs = Object.fromEntries(resultCols.map((col: string) => [col, result.getChild(col)]));

			for (let i = 0; i < result.numRows; i++) {
				const h3index = String(h3Vec!.get(i));
				const values: Record<string, any> = {};
				for (const col of resultCols) {
					const val = colVecs[col]?.get(i);
					if (val === null || val === undefined) continue;
					const num = Number(val);
					values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
				}
				data.set(h3index, values);
				try {
					const boundary = cellToBoundary(h3index);
					const coords = boundary.map(([lat, lng]) => [lng, lat]);
					coords.push(coords[0]);
					bounds.set(h3index, coords);
				} catch { /* skip invalid h3 */ }
			}

			setDeptCache(compareCacheKey, { data, centroids: new Map(), boundaries: bounds, bbox: HexStore.bboxFromBounds(bounds) });

			this.compareVisibleData = data;
			this.compareBoundaryCache = bounds;
			this.compareDpto = dpto;
			this.compareTerritoryPrefix = comparePrefix;
			this.compareDeptBbox = HexStore.bboxFromBounds(bounds);
			this.compareDataVersion++;
		} catch (e) {
			console.warn('Failed to load compare dept data:', e);
			this.loadError = 'dataLoadFailed';
		}
	}

	// Tracks in-flight / completed global loads by `${layer}:${target}:${prefix}` so the
	// regional/compare effects (which re-fire on every reactive tick) don't launch N
	// redundant full-global SELECTs. Cleared on layer/territory change so switching the
	// regional territory still reloads. This reactive thrash — not the per-dept path —
	// is what made carbon feel broken in regional/compare mode (cf=py).
	private _globalLoadState = new Map<string, 'loading' | 'done'>();

	private async loadGlobalInto(
		layer: HexLayerConfig,
		prefix: string,
		target: 'primary' | 'compare' | 'regional',
		cells?: string[]
	): Promise<void> {
		// Never fire before DuckDB is ready: the effects retry every tick, and a SELECT
		// before init throws → gets caught → re-fires in a loop.
		if (!isReady()) return;

		// Viewport mode (B): `cells` present → load ONLY those H3 cells (WHERE IN) instead
		// of the whole territory global (giant PY territories are 700K-850K hex). No
		// _globalLoadState dedup — cells change on every pan. An empty cell set (zoomed out
		// past the cap) clears the slot's display; the caller keeps the wanted prefix so it
		// reloads on zoom-in.
		const viewport = cells !== undefined;
		if (viewport && cells!.length === 0) {
			if (target === 'compare') {
				this.compareVisibleData = new Map();
				this.compareBoundaryCache = new Map();
				this.compareDeptBbox = null;
				this.compareDataVersion++;
			} else if (target === 'regional') {
				this.regionalVisibleData = new Map();
				this.regionalBoundaryCache = new Map();
				this.regionalDataVersion++;
			}
			return;
		}

		const stateKey = `${layer.id}:${target}:${prefix}`;
		if (!viewport) {
			const st = this._globalLoadState.get(stateKey);
			if (st === 'loading' || st === 'done') return; // already loaded or in flight
			this._globalLoadState.set(stateKey, 'loading');
		}
		try {
		// EUDR is a single global dataset (10 provinces) at data/eudr/, not a
		// per-territory parquet — ignore the territory prefix entirely.
		const url = layer.id === 'eudr'
			? getEudrParquetUrl(layer.parquet)
			: getSatGlobalUrl(layer.id, prefix);
		// Project to only the columns the regional/compare UI needs (choropleth +
		// ComparisonPanel + petal + categorical) instead of SELECT * — the global carries
		// ~20 unused baseline/delta/pca columns. Inspect schema first to tolerate drift.
		const actualCols = await getParquetCols(url);
		const wanted = ['h3index', layer.primaryVariable, 'type', 'type_label',
			...layer.variables.flatMap(v => [v.col, v.rawCol]),
			...(layer.petalVars?.map(v => v.col) ?? [])]
			.filter((c): c is string => !!c && actualCols.has(c))
			.filter((c, i, a) => a.indexOf(c) === i);
		const whereClause = viewport
			? ` WHERE h3index IN (${cells!.map(c => `'${c}'`).join(',')})`
			: '';
			const result = await query(`SELECT ${wanted.map(c => `"${c}"`).join(', ')} FROM '${url}'${whereClause}`);
		const data = new Map<string, Record<string, any>>();
		const centroids = new Map<string, [number, number]>();
		const bounds = new Map<string, number[][]>();

		const resultCols = result.schema.fields
			.map((f: any) => f.name)
			.filter((n: string) => n !== 'h3index');
		const h3Vec = result.getChild('h3index');
		const colVecs = Object.fromEntries(resultCols.map((col: string) => [col, result.getChild(col)]));

		for (let i = 0; i < result.numRows; i++) {
			const h3index = String(h3Vec!.get(i));
			const values: Record<string, any> = {};
			for (const col of resultCols) {
				const val = colVecs[col]?.get(i);
				if (val === null || val === undefined) continue;
				const num = Number(val);
				values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
			}
			data.set(h3index, values);
			try {
				const [lat, lng] = cellToLatLng(h3index);
				centroids.set(h3index, [lng, lat]);
				const boundary = cellToBoundary(h3index);
				const coords = boundary.map(([lat, lng]: [number, number]) => [lng, lat]);
				coords.push(coords[0]);
				bounds.set(h3index, coords);
			} catch { /* skip invalid h3 */ }
		}

		if (target === 'primary') {
			this.visibleData = data;
			this.centroidCache = centroids;
			this.boundaryCache = bounds;
			this.selectedDpto = null;
			this.selectedParquetKey = null;
			this.deptBbox = null;
			this.dataVersion++;
		} else if (target === 'compare') {
			this.compareVisibleData = data;
			this.compareBoundaryCache = bounds;
			this.compareDpto = null;
			this.compareTerritoryPrefix = prefix;
			this.compareDeptBbox = null;
			this.compareDataVersion++;
		} else {
			this.regionalVisibleData = data;
			this.regionalBoundaryCache = bounds;
			this.regionalDataVersion++;
		}
			if (!viewport) this._globalLoadState.set(stateKey, 'done');
		} catch (e) {
			if (!viewport) this._globalLoadState.delete(stateKey); // allow a later retry
			throw e;
		}
	}

	// Register a full-territory global for the compare slot. Rows are NOT loaded here:
	// the moveend effect in +page calls loadGlobalViewport() to fetch only the visible
	// cells (B). Switching to a single-dept compare clears this (see loadCompareDept).
	// Kept async so existing `.catch()` callers keep working.
	async loadFullCompare(comparePrefix: string): Promise<void> {
		const layer = this.activeLayer;
		if (!layer || layer.id === 'eudr') return;
		this.compareDpto = null;
		this.compareTerritoryPrefix = comparePrefix;
		this.compareGlobalPrefix = comparePrefix;
	}

	// Register a full-territory global for the regional (3rd territory) slot. See above.
	async loadRegionalData(prefix: string): Promise<void> {
		const layer = this.activeLayer;
		if (!layer || layer.id === 'flood_risk' || layer.id === 'eudr') return;
		this.regionalGlobalPrefix = prefix;
	}

	// Viewport-scoped load for a registered global slot (compare/regional). Called by the
	// moveend effect in +page with the H3 cells currently in view (empty = too wide → clear).
	async loadGlobalViewport(target: 'compare' | 'regional', prefix: string, cells: string[]): Promise<void> {
		const layer = this.activeLayer;
		if (!layer) return;
		try {
			await this.loadGlobalInto(layer, prefix, target, cells);
		} catch (e) {
			console.warn(`Failed to load ${target} viewport data:`, e);
		}
	}

	// Two-phase prefetch:
	// Phase 1 (immediate): HTTP fetch() to warm browser + CDN cache. Non-blocking.
	// Phase 2 (idle): requestIdleCallback-scheduled DuckDB query + H3 geometry precompute.
	//   Only runs when the browser is idle (no user interaction, no animations).
	//   Processes smallest depts first for fastest cache population.
	//   Never starts a new DuckDB query while user has an active load in progress.
	private _prefetchDeptParquets(layer: HexLayerConfig): void {
		const token = ++this._prefetchToken;
		const prefix = this.territoryPrefix;
		loadDeptSummary(layer.id, prefix).then(summary => {
			if (this._prefetchToken !== token || !summary?.departments) return;
			const depts = (summary.departments as any[]);

			// NOTE: the old "Phase 1" eagerly fetch()'d EVERY dept parquet at once (~17 ×
			// 1.12 MB = ~19 MB for carbon), saturating the network the moment the layer was
			// selected and starving the dept the user actually clicks. Removed — the idle
			// precompute below already warms the cache one dept at a time, and a dept click
			// aborts it (via _prefetchToken bump in loadDepartment).

			// Geometry precompute during idle time (smallest depts first), one at a time.
			const remaining = [...depts].sort((a: any, b: any) => (a.hex_count ?? 0) - (b.hex_count ?? 0));
			this._idlePrecomputeNext(layer, remaining, token, prefix);
		});
	}

	private _idlePrecomputeNext(layer: HexLayerConfig, remaining: any[], token: number, prefix: string): void {
		if (this._prefetchToken !== token || remaining.length === 0) return;
		const reschedule = () => {
			if (this._prefetchToken !== token || remaining.length === 0) return;
			if (typeof requestIdleCallback !== 'undefined') {
				requestIdleCallback(() => this._idlePrecomputeNext(layer, remaining, token, prefix), { timeout: 4000 });
			} else {
				setTimeout(() => this._idlePrecomputeNext(layer, remaining, token, prefix), 300);
			}
		};
		// Don't start a DuckDB query if user is actively loading something
		if (this.loading) { reschedule(); return; }

		const dept = remaining.shift()!;
		const key = dept.parquetKey as string;
		if (!key) { reschedule(); return; }
		const cacheKey = `${layer.id}:${key}:${prefix}`;
		if (deptDataCache.has(cacheKey)) { reschedule(); return; }

		let url: string;
		if (layer.id === 'flood_risk') url = getFloodDptoUrl(key, prefix);
		else if (layer.parquet?.startsWith('sat_')) url = getSatDptoUrl(layer.id, key, prefix);
		else url = getScoresDptoUrl(key, prefix);

		query(`SELECT * FROM '${url}'`).then(result => {
			if (this._prefetchToken !== token) return;
			const data = new Map<string, Record<string, any>>();
			const centroids = new Map<string, [number, number]>();
			const boundaries = new Map<string, number[][]>();
			const resultCols = result.schema.fields.map((f: any) => f.name).filter((n: string) => n !== 'h3index');
			const h3Vec = result.getChild('h3index');
			const colVecs = Object.fromEntries(resultCols.map((col: string) => [col, result.getChild(col)]));
			for (let i = 0; i < result.numRows; i++) {
				const h3index = String(h3Vec!.get(i));
				try {
					const [lat, lng] = cellToLatLng(h3index);
					const values: Record<string, any> = {};
					for (const col of resultCols) {
						const val = colVecs[col]?.get(i);
						if (val === null || val === undefined) continue;
						const num = Number(val);
						values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
					}
					data.set(h3index, values);
					centroids.set(h3index, [lng, lat]);
					const boundary = cellToBoundary(h3index);
					const coords = boundary.map(([lat, lng]) => [lng, lat]);
					coords.push(coords[0]);
					boundaries.set(h3index, coords);
				} catch { /* skip */ }
			}
			// Edge-gap fill for Misiones AR (has boundary polygons)
			const deptName = dept.dpto ?? dept.distrito ?? dept.municipio;
			if (prefix === '' && deptName) {
				const deptFeature = findDeptFeature(deptName, prefix);
				if (deptFeature?.geometry) {
					const geom = deptFeature.geometry;
					const polygons: number[][][][] = geom.type === 'MultiPolygon' ? geom.coordinates
						: geom.type === 'Polygon' ? [geom.coordinates] : [];
					for (const poly of polygons) {
						try {
							for (const h3index of polygonToCells(poly as any, 9, true)) {
								if (data.has(h3index)) continue;
								try {
									const [lat, lng] = cellToLatLng(h3index);
									data.set(h3index, {});
									centroids.set(h3index, [lng, lat]);
									const boundary = cellToBoundary(h3index);
									const coords = boundary.map(([lat, lng]) => [lng, lat]);
									coords.push(coords[0]);
									boundaries.set(h3index, coords);
								} catch { /* skip */ }
							}
						} catch { /* skip */ }
					}
				}
			}
			setDeptCache(cacheKey, { data, centroids, boundaries, bbox: HexStore.bboxFromCentroids(centroids) });
			reschedule(); // schedule next dept
		}).catch(() => reschedule());
	}

	clearRegionalData(): void {
		this.regionalGlobalPrefix = null; // stop the viewport global refresh for this slot
		if (this.regionalVisibleData.size === 0) return;
		this.regionalVisibleData = new Map();
		this.regionalBoundaryCache = new Map();
		this.regionalDataVersion++;
	}

	private regionalChoroplethEntries_compute(): { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] {
		if (!this.activeLayer) return [];
		const pv = this.activeLayer.primaryVariable;
		const entries: { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] = [];
		for (const [h3index, data] of this.regionalVisibleData) {
			entries.push({ h3index, value: (data[pv] ?? 0) as number, properties: data as Record<string, number>, boundary: this.regionalBoundaryCache.get(h3index) });
		}
		return entries;
	}

	get regionalChoroplethEntries(): { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] {
		if (this.regionalDataVersion === this._regionalEntriesVersion) return this._regionalEntriesCache;
		const result = this.regionalChoroplethEntries_compute();
		this._regionalEntriesVersion = this.regionalDataVersion;
		this._regionalEntriesCache = result;
		return result;
	}

	private static bboxFromBounds(bounds: Map<string, number[][]>): [number, number, number, number] | null {
		if (bounds.size === 0) return null;
		let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
		for (const coords of bounds.values()) {
			for (const [lng, lat] of coords) {
				if (lng < minLng) minLng = lng;
				if (lat < minLat) minLat = lat;
				if (lng > maxLng) maxLng = lng;
				if (lat > maxLat) maxLat = lat;
			}
		}
		return [minLng, minLat, maxLng, maxLat];
	}

	// Use centroids (not vertices) to avoid border hexagon vertex overreach across international boundaries
	private static bboxFromCentroids(centroids: Map<string, [number, number]>): [number, number, number, number] | null {
		if (centroids.size === 0) return null;
		let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
		for (const [lng, lat] of centroids.values()) {
			if (lng < minLng) minLng = lng;
			if (lat < minLat) minLat = lat;
			if (lng > maxLng) maxLng = lng;
			if (lat > maxLat) maxLat = lat;
		}
		return [minLng, minLat, maxLng, maxLat];
	}

	clearCompareDept(): void {
		this.compareGlobalPrefix = null; // stop the viewport global refresh for this slot
		if (this.compareDpto === null && this.compareVisibleData.size === 0) return;
		this.compareVisibleData = new Map();
		this.compareBoundaryCache = new Map();
		this.compareDpto = null;
		this.compareTerritoryPrefix = null;
		this.compareDeptBbox = null;
		this.compareDataVersion++;
	}

	private compareChoroplethEntries_compute(): { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] {
		if (!this.activeLayer) return [];
		const pv = this.activeLayer.primaryVariable;
		const entries: { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] = [];
		for (const [h3index, data] of this.compareVisibleData) {
			const value = (data[pv] ?? 0) as number;
			entries.push({ h3index, value, properties: data as Record<string, number>, boundary: this.compareBoundaryCache.get(h3index) });
		}
		return entries;
	}

	get compareChoroplethEntries(): { h3index: string; value: number; properties: Record<string, number>; boundary?: number[][] }[] {
		if (this.compareDataVersion === this._compareEntriesVersion) return this._compareEntriesCache;
		const result = this.compareChoroplethEntries_compute();
		this._compareEntriesVersion = this.compareDataVersion;
		this._compareEntriesCache = result;
		return result;
	}

	backToDepartments() {
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.selectedHexResLevel = null;
		this.lodDeptActive = false;
		this.lodDeptContext = null;
		this.deptBbox = null;
		this.clearSelection();
		this.clearHexZones();
	}

	async loadVisibleData() {
		const layer = this.activeLayer;
		if (!layer || !isReady()) return;
		// EUDR never bulk-loads — viewport-only (see loadEudrViewport)
		if (layer.perDepartment || layer.id === 'eudr') return;

		this.loading = true;
		this.loadError = null;

		try {
			await this.loadBaseResolution(layer);
			// Fire-and-forget: pre-cache provincial averages so lasso zones are instant
			this.ensureProvincialAvg().catch(() => {});
		} catch (e) {
			console.warn('Failed to load hex data:', e);
			this.loadError = 'dataLoadFailed';
		}

		this.loading = false;
	}

	private async loadBaseResolution(layer: HexLayerConfig) {
		const url = this.layerGlobalUrl(layer);
		if (!url) return;

		const baseCols = layer.variables.map(v => v.col);
		const allCols = new Set(baseCols);
		if (layer.temporal) {
			for (const col of baseCols) {
				allCols.add(getTemporalCol(col, 'baseline'));
				allCols.add(getTemporalCol(col, 'delta'));
			}
		}
		const cols = [...allCols].join(', ');
		const result = await query(
			`SELECT h3index, ${cols} FROM '${url}'`
		);

		const data = new Map<string, Record<string, any>>();
		const centroids = new Map<string, [number, number]>();
		const boundaries = new Map<string, number[][]>();

		const h3Vec = result.getChild('h3index');
		const allColVecs = Object.fromEntries(
			[...allCols].map((col: string) => [col, result.getChild(col)])
		);

		for (let i = 0; i < result.numRows; i++) {
			const h3index = String(h3Vec!.get(i));
			const values: Record<string, any> = {};
			for (const col of allCols) {
				const val = allColVecs[col]?.get(i);
				if (val === null || val === undefined) continue;
				const num = Number(val);
				values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
			}
			data.set(h3index, values);

			// Pre-compute geometry once for all subsequent operations
			try {
				const [lat, lng] = cellToLatLng(h3index);
				centroids.set(h3index, [lng, lat]);
				const boundary = cellToBoundary(h3index);
				const coords = boundary.map(([lat, lng]) => [lng, lat]);
				coords.push(coords[0]); // close ring
				boundaries.set(h3index, coords);
			} catch { /* skip invalid h3 */ }
		}

		this.visibleData = data;
		this.centroidCache = centroids;
		this.boundaryCache = boundaries;
		this.dataVersion++;

		// Persist in module-level cache for instant re-activation
		layerDataCache.set(layer.id, { data, centroids, boundaries, provincialAvg: null });
	}

	// EUDR: load only the H3 cells in the current viewport (scalable —
	// never loads the whole multi-province dataset at once).
	async loadEudrViewport(cells: string[]) {
		const layer = this.activeLayer;
		if (!layer || layer.id !== 'eudr' || !isReady()) return;
		if (cells.length === 0) {
			this.visibleData = new Map();
			this.boundaryCache = new Map();
			this.centroidCache = new Map();
			this.dataVersion++;
			return;
		}
		this.loading = true;
		try {
			const url = getEudrParquetUrl(layer.parquet);
			const colNames = layer.variables.map(v => v.col);
			const inList = cells.map(c => `'${c}'`).join(',');
			const result = await query(
				`SELECT h3index, ${colNames.join(', ')} FROM '${url}' WHERE h3index IN (${inList})`
			);
			const data = new Map<string, Record<string, any>>();
			const centroids = new Map<string, [number, number]>();
			const boundaries = new Map<string, number[][]>();
			const h3Vec = result.getChild('h3index');
			const colVecs = Object.fromEntries(colNames.map(c => [c, result.getChild(c)]));
			for (let i = 0; i < result.numRows; i++) {
				const h3index = String(h3Vec!.get(i));
				const values: Record<string, any> = {};
				for (const c of colNames) {
					const v = colVecs[c]?.get(i);
					if (v === null || v === undefined) continue;
					const n = Number(v);
					values[c] = Number.isFinite(n) && typeof v !== 'string' ? n : String(v);
				}
				data.set(h3index, values);
				try {
					const [lat, lng] = cellToLatLng(h3index);
					centroids.set(h3index, [lng, lat]);
					const boundary = cellToBoundary(h3index);
					const coords = boundary.map(([lat, lng]) => [lng, lat]);
					coords.push(coords[0]);
					boundaries.set(h3index, coords);
				} catch { /* skip invalid h3 */ }
			}
			this.visibleData = data;
			this.centroidCache = centroids;
			this.boundaryCache = boundaries;
			this.dataVersion++;
		} catch (e) {
			console.warn('EUDR viewport load failed:', e);
		} finally {
			this.loading = false;
		}
	}

	// ── Selection ────────────────────────────────────────────────────────

	private static goldenAngleColor(i: number): string {
		const hue = Math.round((i * 137.508) % 360);
		return `hsl(${hue}, 72%, 63%)`;
	}

	selectHex(h3index: string) {
		if (this.selectedHexes.has(h3index)) return;
		const color = HexStore.goldenAngleColor(this.colorIndex);
		this.colorIndex++;
		const data = this.visibleData.get(h3index) ?? {};
		const updated = new Map(this.selectedHexes);
		updated.set(h3index, { color, data });
		this.selectedHexes = updated;
	}

	selectCompareHex(h3index: string) {
		if (this.selectedHexes.has(h3index)) return;
		const data = this.compareVisibleData.get(h3index) ?? {};
		const updated = new Map(this.selectedHexes);
		// Amber color to distinguish compare territory hexes visually
		updated.set(h3index, { color: '#f59e0b', data });
		this.selectedHexes = updated;
	}

	selectRegionalHex(h3index: string) {
		if (this.selectedHexes.has(h3index)) return;
		const data = this.regionalVisibleData.get(h3index) ?? {};
		const updated = new Map(this.selectedHexes);
		// Violet to distinguish Itapúa (PY) from AR hexes
		updated.set(h3index, { color: '#8b5cf6', data });
		this.selectedHexes = updated;
	}

	toggleRegionalHex(h3index: string) {
		if (this.selectedHexes.has(h3index)) {
			this.deselectHex(h3index);
		} else {
			this.selectRegionalHex(h3index);
		}
	}

	deselectHex(h3index: string) {
		if (!this.selectedHexes.has(h3index)) return;
		const updated = new Map(this.selectedHexes);
		updated.delete(h3index);
		this.selectedHexes = updated;
		if (updated.size === 0) this.colorIndex = 0;
	}

	toggleHex(h3index: string) {
		if (this.selectedHexes.has(h3index)) {
			this.deselectHex(h3index);
		} else {
			this.selectHex(h3index);
		}
	}

	hasHex(h3index: string): boolean {
		return this.selectedHexes.has(h3index);
	}

	clearSelection() {
		this.selectedHexes = new Map();
		this.colorIndex = 0;
	}

	// ── Choropleth entries ────────────────────────────────────────────────

	// Private compute method (for type inference in memoization above)
	private choroplethEntries_compute() {
		if (!this.activeLayer) return [];
		const effectivePrimary = this.activeLayer.temporal && this.temporalMode !== 'current'
			? getTemporalCol(this.activeLayer.primaryVariable, this.temporalMode)
			: this.activeLayer.primaryVariable;
		const isDelta = this.activeLayer.temporal && this.temporalMode === 'delta';
		const entries: { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean }[] = [];
		for (const [h3index, data] of this.visibleData) {
			const raw = data[effectivePrimary];
			const hasData = raw !== undefined && raw !== null &&
				(typeof raw !== 'number' || Number.isFinite(raw));
			const value: number | null = hasData ? Number(raw) : null;
			if (isDelta && hasData && value === 0) continue;
			entries.push({
				h3index,
				value,
				properties: data,
				boundary: this.boundaryCache.get(h3index),
				nodata: !hasData
			});
		}
		return entries;
	}

	get choroplethEntries(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean }[] {
		if (this.dataVersion === this._entriesVersion) return this._entriesCache;
		const result = this.choroplethEntries_compute();
		this._entriesVersion = this.dataVersion;
		this._entriesCache = result;
		return result;
	}

	// ── Hex zone (lasso) operations ──────────────────────────────────────

	findHexesInPolygon(polygon: [number, number][]): string[] {
		// Pre-compute bounding box of the lasso polygon to skip ~90% of candidates
		let minLng = Infinity, maxLng = -Infinity, minLat = Infinity, maxLat = -Infinity;
		for (const [lng, lat] of polygon) {
			if (lng < minLng) minLng = lng;
			if (lng > maxLng) maxLng = lng;
			if (lat < minLat) minLat = lat;
			if (lat > maxLat) maxLat = lat;
		}

		const result: string[] = [];
		for (const [h3index, centroid] of this.centroidCache) {
			const [lng, lat] = centroid;
			// Fast bbox rejection
			if (lng < minLng || lng > maxLng || lat < minLat || lat > maxLat) continue;
			if (pointInPolygon([lng, lat], polygon)) {
				result.push(h3index);
			}
		}
		return result;
	}

	private async ensureProvincialAvg(): Promise<number[]> {
		if (this.provincialAvg) return this.provincialAvg;
		if (!this.activeLayer) return [];

		const layer = this.activeLayer;
		const dataUrl = this.layerGlobalUrl(layer);
		if (!dataUrl) return [];

		const numVars = this.numericVariables;
		if (numVars.length === 0) return [];

		try {
			// Inspect actual parquet schema first to avoid Binder errors when config
			// and parquet columns are out of sync (stale config / new pipeline output).
			const schemaResult = await query(`SELECT * FROM '${dataUrl}' LIMIT 0`);
			const actualCols = new Set(schemaResult.schema.fields.map((f: any) => f.name as string));
			const availableVars = numVars.filter(v => actualCols.has(v.col));

			if (availableVars.length === 0) {
				this.provincialAvg = numVars.map(() => 1);
			} else {
				const aggExprs = availableVars.map(v => `AVG(${v.col}) as avg_${v.col}`).join(', ');
				const whereClause = actualCols.has(layer.primaryVariable)
					? `WHERE ${layer.primaryVariable} IS NOT NULL`
					: '';
				const sql = `SELECT ${aggExprs} FROM '${dataUrl}' ${whereClause}`;
				const result = await query(sql);
				const row = result.get(0)!.toJSON() as Record<string, any>;
				this.provincialAvg = numVars.map(v =>
					actualCols.has(v.col) ? (Number(row[`avg_${v.col}`]) || 1) : 1
				);
			}
		} catch (e) {
			console.warn('ensureProvincialAvg failed (schema mismatch?), using defaults:', e);
			this.provincialAvg = numVars.map(() => 1);
		}

		// Update persistent cache with provincial avg
		const cached = layerDataCache.get(layer.id);
		if (cached) cached.provincialAvg = this.provincialAvg;
		return this.provincialAvg;
	}

	async ensureColorDomain(): Promise<[number, number] | null> {
		if (this.colorDomain) return this.colorDomain;
		if (!this.activeLayer) return null;

		const layer = this.activeLayer;

		// Fixed physical domain declared in config → no remote query at all.
		// Avoids the slow MIN/MAX scan on heavy single-row-group parquets (carbon)
		// and keeps the color scale identical across territories.
		if (layer.fixedColorDomain) {
			this.colorDomain = layer.fixedColorDomain;
			return this.colorDomain;
		}

		const dataUrl = this.layerGlobalUrl(layer);
		if (!dataUrl) return null;

		const pv = layer.primaryVariable;
		try {
			// No WHERE … IS NOT NULL: MIN/MAX already ignore NULLs, and the predicate
			// blocks DuckDB's footer-statistics pushdown (forces a full column scan).
			const sql = `SELECT MIN(${pv}) as lo, MAX(${pv}) as hi FROM '${dataUrl}'`;
			const result = await query(sql);
			const row = result.get(0)!.toJSON() as Record<string, any>;
			const lo = Number(row.lo) || 0;
			const hi = Number(row.hi) || 100;
			if (hi > lo) {
				this.colorDomain = [lo, hi];
			}
			return this.colorDomain;
		} catch {
			return null;
		}
	}

	private normalize(rawValues: number[], provAvg: number[]): number[] {
		return rawValues.map((v, i) => {
			const avg = provAvg[i];
			if (avg === 0) return 50;
			return Math.min(100, Math.max(0, (v / avg) * 50));
		});
	}

	async createHexZone(h3indices: string[], polygon: [number, number][]): Promise<void> {
		if (h3indices.length === 0 || !this.activeLayer) return;
		if (!isReady()) return;

		const layer = this.activeLayer;
		const idx = this.hexZones.length % ZONE_COLORS.length;
		const id = ZONE_LABELS[idx] || String.fromCharCode(65 + this.hexZones.length);
		const color = ZONE_COLORS[idx];

		try {
			const provAvg = await this.ensureProvincialAvg();

			// Compute averages from visibleData (numeric vars only)
			const numVars = this.numericVariables;
			const rawValues = new Array(numVars.length).fill(0);
			let count = 0;
			for (const h3index of h3indices) {
				const data = this.visibleData.get(h3index);
				if (!data) continue;
				count++;
				for (let v = 0; v < numVars.length; v++) {
					rawValues[v] += data[numVars[v].col] || 0;
				}
			}
			if (count > 0) {
				for (let v = 0; v < rawValues.length; v++) {
					rawValues[v] /= count;
				}
			}

			const normalizedValues = this.normalize(rawValues, provAvg);

			const zone: HexZone = {
				id,
				color,
				h3indices,
				polygon,
				stats: {
					hexCount: h3indices.length,
					rawValues,
					normalizedValues,
				},
			};

			this.hexZones = [...this.hexZones, zone];
		} catch (e) {
			console.warn('Failed to create hex zone:', e);
		}
	}

	removeHexZone(id: string) {
		this.hexZones = this.hexZones.filter(z => z.id !== id);
	}

	clearHexZones() {
		this.hexZones = [];
	}

	// ── Petal chart data ─────────────────────────────────────────────────

	get petalLayers(): Array<{ values: number[]; color: string }> {
		return this.hexZones.map(z => ({
			values: z.stats.normalizedValues,
			color: z.color,
		}));
	}

	get petalLabels(): string[] {
		return this.numericVariables.map(v => i18n.t(v.labelKey));
	}

	// ── Selection petal data (individual hex clicks) ─────────────────────

	get selectionPetalLayers(): Array<{ values: number[]; color: string }> {
		if (!this.activeLayer || this.selectedHexes.size === 0 || !this.provincialAvg) return [];
		const provAvg = this.provincialAvg;
		const vars = this.numericVariables;
		const result: Array<{ values: number[]; color: string }> = [];
		for (const [, sel] of this.selectedHexes) {
			const rawValues = vars.map(v => sel.data[v.col] ?? 0);
			const normalizedValues = this.normalize(rawValues, provAvg);
			result.push({ values: normalizedValues, color: sel.color });
		}
		return result;
	}

	async ensureProvincialAvgLoaded(): Promise<void> {
		await this.ensureProvincialAvg();
	}

	// ── Full clear ───────────────────────────────────────────────────────

	clearLoadError() {
		this.loadError = null;
	}

	clearAll() {
		this.activeLayer = null;
		this.visibleData = new Map();
		this.centroidCache = new Map();
		this.boundaryCache = new Map();
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.deptBbox = null;
		this.loadError = null;
		this.temporalMode = 'current';
		this.dataVersion++;
		this.selectedHexes = new Map();
		this.hexZones = [];
		this.colorIndex = 0;
		this.provincialAvg = null;
		this.colorDomain = null;
		this.clearCompareDept();
		this.clearRegionalData();
	}
}
