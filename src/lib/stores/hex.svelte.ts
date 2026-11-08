import { query, isReady } from '$lib/stores/duckdb';
import { PARQUETS, HEX_LAYER_REGISTRY, TERRITORY_REGISTRY, getFloodDptoUrl, getSatDptoUrl, getSatGlobalUrl, getTemporalCol, type HexLayerConfig, type HexVariable, type TemporalMode, type ColorClasses, type MacizoFocus } from '$lib/config';
import { NODATA_HEX, HARVEST_HEX } from '$lib/utils/sello';
import { pointInPolygon } from '$lib/utils/geometry';
import { aggregateRows, weightColOf } from '$lib/utils/zoneStats';
import { urlOutline, type UrlZone } from '$lib/utils/zoneCodec';
import { findDeptFeature, ensureArBoundaries, ensureBrBoundaries, isArDeptTerritory } from '$lib/utils/deptBoundaries';
import { loadDeptSummary } from '$lib/utils/deptSummaries';
import { i18n } from '$lib/stores/i18n.svelte';
import { cellToLatLng, cellToBoundary, polygonToCells, cellToParent } from 'h3-js';

// ── LOD (level of detail) for giant departments ──────────────────────────────
// Chaco/Boqueron districts reach 400K-750K res-9 hexes. Building one polygon per
// res-9 cell on the main thread (cellToBoundary loop + setData clone) freezes the
// UI for seconds. Above LOD_ACTIVATION_ROWS we aggregate to a coarser H3 resolution
// chosen so the rendered polygon count lands near LOD_TARGET_POLYGONS — visually
// near-identical at department-overview zoom, but ~10-50x fewer polygons.
// Lowered (was 60000/15000): a CPU profile showed dept-select time is dominated by
// MapLibre's synchronous tessellation + GPU upload of the choropleth — i.e. the
// rendered POLYGON COUNT, not JS. Misiones depts (10-35k res-9 hexes) sat under the
// old 60k gate and rendered full res-9 (+ edge-fill), costing several seconds each.
// Aggregating to ~6k coarse polygons at overview (res-9 restored per-viewport on
// zoom-in) cuts that ~5-6x with no visible change at department scale.
const LOD_ACTIVATION_ROWS = 12000;
const LOD_TARGET_POLYGONS = 6000;
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

// Zone identities, kept away from tierra (#c4572f), which marks the live chart selection.
const ZONE_COLORS = ['#60a5fa', '#22c55e', '#eab308', '#a855f7', '#2dd4bf', '#f472b6'];
// Letters are unique across departments (the zone C of Oberá stays C next to Capital's A and B);
// colours are picked among the zones of the same department.
const ZONE_LABELS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
/** sessionStorage key of the saved zones: they outlive a reload, not the tab (author, 8-nov-2026). */
const ZONES_STORAGE_KEY = 'spatia.zones.v1';

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

// Charts analyse the whole department at res-9 even when the map draws a coarse
// overview (LOD): the overview is for drawing only. The res-9 values (no geometry)
// live in their own small cache — they are 10-50× the overview's rows.
// Above ANALYSIS_MAX_ROWS (giant PY districts, ~445K hex) charts fall back to the
// coarse cells and say so.
const ANALYSIS_MAX_ROWS = 200_000;
const ANALYSIS_CACHE_MAX = 4;
const analysisCache = new Map<string, Map<string, Record<string, any>>>();
function setAnalysisCache(key: string, value: Map<string, Record<string, any>>): void {
	if (analysisCache.has(key)) analysisCache.delete(key);
	analysisCache.set(key, value);
	if (analysisCache.size > ANALYSIS_CACHE_MAX) {
		const oldest = analysisCache.keys().next().value;
		if (oldest !== undefined) analysisCache.delete(oldest);
	}
}

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
	/** Cells of the zone with at least one value in the layer (0 = layer has no data there). */
	cellsWithData: number;
	rawValues: number[];
	/** Σ over the zone's cells, for count variables (aggregation 'sum'): «2269 buildings
	 *  on low ground in the zone» reads right where the per-cell mean (37.8) does not. */
	totals: number[];
	normalizedValues: number[];
}

export interface HexZone {
	id: string;
	color: string;
	h3indices: string[];
	/** Lasso outline; null for zones saved from a chart selection (not contiguous). */
	polygon: [number, number][] | null;
	/** Where the zone came from, shown next to its letter: «Lazo», «Moran: recuadro»… */
	origin: string;
	stats: HexZoneStats;
	/** Department the zone was drawn in (territory prefix + name): it follows the user
	 *  across layers and lenses while that department stays open. */
	dept: string | null;
	/** Layer whose variables `stats` holds; another active layer means «recompute». */
	layerId: string;
	/** The outline as the URL carries it (utils/zoneCodec.urlOutline); null for chart zones. */
	outline: string | null;
	/** Hexagons the zone had in the link it was opened from (absent when drawn here). */
	sharedN?: number;
	/** The user's name for it («Barrio Itaembé»); the letter stays its short identity. */
	name?: string;
}

/** A zone name as kept and put in the link: trimmed, 40 characters, no «|» (the `zn` separator). */
export function cleanZoneName(s: string): string {
	return s.replace(/\|/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 40).trim();
}

/** Hexagons picked in a chart (histogram, Moran, bivariate, profile…), not yet saved. */
export interface ChartSelection {
	h3s: string[];
	origin: string;
}

const NON_NUMERIC_COLS = new Set(['type', 'type_label', 'pca_1', 'pca_2', 'pca_3', 'score', 'risk_score', 'territorial_type']);

export interface DeptFill {
	territory: string;                                    // territory id
	values: { name: string; value: number | null }[];    // ranking order
	max: number;                                          // max |value|, the bars' scale
	signed: boolean;
	decimals: number;
	unit: string;
	title: string;                                        // the reading's question
}

export class HexStore {
	activeLayer: HexLayerConfig | null = $state(null);
	visibleData: Map<string, Record<string, any>> = $state(new Map());
	// What the charts analyse: the whole department at res-9, stable under zoom/pan.
	// Null = same as visibleData (non-LOD loads are already res-9 and never swapped).
	analysisData: Map<string, Record<string, any>> | null = $state.raw(null);
	// Coarse res when even the analysis had to aggregate (giant districts), else null.
	analysisResLevel: number | null = $state(null);
	get chartData(): Map<string, Record<string, any>> {
		const src = this.analysisData ?? this.visibleData;
		const f = this.deptFilter;
		if (!f) return src;
		const c = this._deptChart;
		if (c && c.src === src && c.filter === f) return c.out;
		const out = new Map<string, Record<string, any>>();
		for (const h of f) { const d = src.get(h); if (d) out.set(h, d); }
		this._deptChart = { src, filter: f, out };
		return out;
	}
	// Territory-wide layers (censo_temporal) narrowed to one department picked in
	// their own list: charts and map read only these cells, like the per-department
	// layers. Cells assigned by centre-in-polygon, the rule of the Hexoma.
	deptFilter: Set<string> | null = $state.raw(null);
	deptFilterName: string | null = $state(null);
	private _deptChart: { src: Map<string, Record<string, any>>; filter: Set<string>; out: Map<string, Record<string, any>> } | null = null;
	setDeptFilter(h3s: Iterable<string> | null, name: string | null = null) {
		// Another department in the layer's own list: its zones stay saved (savedZones) and
		// syncZones brings back the ones of the department now open.
		const key = h3s && name ? `${this.territoryPrefix}|${name}` : null;
		if (key && this.hexZones.length && this.hexZones[0].dept !== key) this.stashZones();
		if (key && name) this.workDept = { prefix: this.territoryPrefix, name };
		this.deptFilter = h3s ? new Set(h3s) : null;
		this.deptFilterName = h3s ? name : null;
		this._deptChart = null;
		this.dataVersion++;
	}
	selectedHexes: Map<string, HexSelectionData> = $state(new Map());
	hexZones: HexZone[] = $state([]);
	// Live selection from a chart, until it is saved as a zone or cleared.
	chartSelection: ChartSelection | null = $state.raw(null);
	setChartSelection(h3s: string[], origin = '') {
		this.chartSelection = h3s.length ? { h3s, origin } : null;
	}
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
	// Res-9 cells already loaded for the open LOD department (accumulated across
	// pans/zooms, capped): a move only queries what is new, and the map is not
	// replaced wholesale on every moveend (that was the zoom flicker).
	private lodDetail: {
		ctx: object; tried: Set<string>;
		data: Map<string, Record<string, any>>; centroids: Map<string, [number, number]>; boundaries: Map<string, number[][]>;
	} | null = null;
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
		return this.activeLayer?.variables.filter(v => !NON_NUMERIC_COLS.has(v.col) && !v.hidden) ?? [];
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

	// Color scale for layers with regionalColorDomain (see HexLayerConfig).
	scaleMode: 'provincial' | 'regional' = $state('provincial');

	setScaleMode(mode: 'provincial' | 'regional') {
		if (this.scaleMode === mode) return;
		this.scaleMode = mode;
		this.colorDomain = null; // recomputed by ensureColorDomain for the new scale
	}

	/** Territory id of the active parquet prefix ('' = misiones). */
	get territoryId(): string {
		return Object.values(TERRITORY_REGISTRY).find((t) => t.parquetPrefix === this.territoryPrefix)?.id ?? 'misiones';
	}

	setTemporalMode(mode: TemporalMode) {
		// Two-scale layers (PM2.5): the baseline is drawn on the fixed regional range, since its values run
		// above the current period's provincial P2–P98 and the map saturated (Corrientes: base 13,5–16,9
		// over 8,4–13,5 µg/m³). Re-resolved here so the domain is set before the repaint (that branch of
		// ensureColorDomain returns without awaiting). Decided by the author on 6-nov-2026.
		const wasBase = this.temporalMode === 'baseline';
		this.temporalMode = mode;
		if (this.activeLayer?.regionalColorDomain && wasBase !== (mode === 'baseline')) {
			this.colorDomain = null;
			void this.ensureColorDomain();
		}
		this.dataVersion++;
	}

	setLayer(layerId: string | null) {
		// The radios/cells clicked in the open department follow to the new layer (syncSelection).
		this.stashSelection();
		this.deptFilter = null;
		this.deptFilterName = null;
		this._globalLoadState.clear(); // layer changed → allow regional/compare reloads for the new layer
		this.primaryOverride = null; // re-resolved against the new layer's parquet schema
		this.colorVariable = null;   // the new layer's panel sets its own
		this.macizoFocus = null;
		this.macizoChartFocus = null;
		this.deptFill = null;
		this.deptFillPick = null;
		this.hexHidden = false;
		if (!layerId) {
			this.activeLayer = null;
			this.visibleData = new Map();
			this.analysisData = null;
			this.analysisResLevel = null;
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
			this.stashZones();
			this.chartSelection = null;
			return;
		}
		const cfg = HEX_LAYER_REGISTRY[layerId];
		if (!cfg) return;
		this.stashZones();
		this.activeLayer = cfg;
		this.temporalMode = 'current';
		this.scaleMode = 'provincial';
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.selectedHexResLevel = null;
		this.lodDeptActive = false;
		this.lodDeptContext = null;
		this.deptBbox = null;
		this.clearCompareDept();
		this.clearRegionalData();

		// Per-department layers: don't load all data, wait for department selection.
		// Background-fetch all dept parquets so the first click is fast.
		if (cfg.perDepartment) {
			this.loading = false;
			if (!cfg.macizos) this._prefetchDeptParquets(cfg);   // macizo layers have no per-dept parquet
			return;
		}

		// Restore from persistent cache if available (instant re-activation)
		const cached = layerDataCache.get(layerId);
		if (cached) {
			this.visibleData = cached.data;
			this.centroidCache = cached.centroids;
			this.analysisData = null;
			this.analysisResLevel = null;
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

	// Per-territory primary-variable fallback. Some layers declare a primary that
	// only exists in a subset of territories (economic_activity paints VIIRS in
	// Misiones, but the AR-census parquets carry only census columns). When the
	// loaded parquet lacks the configured primary, fall back to the first numeric
	// variable the parquet does have — otherwise every hex renders "Sin cobertura"
	// and MIN/MAX color-domain queries throw Binder errors.
	primaryOverride: string | null = $state(null);

	// Variable the map paints instead of the primary, set by the panel (flood: the
	// active ranking tab's per-hex relief variable). Used only when the loaded
	// parquet has the column, so a territory not yet published keeps the primary.
	colorVariable: string | null = $state(null);
	// Macizo layers: the stands the active reading is about (deptRank.macizoFocus); the rest paint gray.
	macizoFocus: MacizoFocus | null = $state(null);
	// A column picked in the macizo charts (CosechaGraficos): replaces the tab's focus until cleared
	// (tab, department or layer change, Esc, or the same column again).
	macizoChartFocus: MacizoFocus | null = $state(null);
	get macizoPaintFocus(): MacizoFocus | null { return this.macizoChartFocus ?? this.macizoFocus; }
	loadedCols: Set<string> = $state(new Set());

	// Province view: the departments painted with the active ranking reading
	// (same values and linear scale as the bars). Set by the panel, null otherwise.
	deptFill: DeptFill | null = $state(null);
	// Province view of the macizo layer: a chart column picked → departments painted with that column's hectares.
	deptFillPick: DeptFill | null = $state(null);
	get mapDeptFill(): DeptFill | null { return this.deptFillPick ?? this.deptFill; }
	setDeptFillPick(f: DeptFill | null) { this.deptFillPick = f; }

	setDeptFill(f: DeptFill | null) {
		this.deptFill = f;
	}

	// An open department whose active ranking reading exists only by department
	// (deptRank.hexNone): the map paints the departments with it and hides the
	// hexagons, instead of painting another reading's cells under its tab.
	hexHidden = $state(false);
	setHexHidden(v: boolean) { this.hexHidden = v; }

	// A department clicked on the map for a layer whose ranking is not the
	// perDepartment one (censo_temporal): the list owns the selection, the map only
	// asks it to pick. The list consumes it (clearDeptPick); `at` drops a request a
	// list never got to (another province's rows still loading when the layer closed).
	// keepMode: the request comes from a link that carried ?tm= (the list keeps that mode).
	deptPick: { name: string; prefix: string; at: number; keepMode?: boolean } | null = $state(null);

	requestDeptPick(name: string, prefix: string, keepMode = false) {
		this.deptPick = { name, prefix, at: Date.now(), keepMode };
	}

	clearDeptPick() {
		this.deptPick = null;
	}

	setMacizoFocus(f: MacizoFocus | null) {
		if (JSON.stringify(f) === JSON.stringify(this.macizoFocus)) return;
		this.macizoFocus = f;
		this.macizoChartFocus = null;
	}

	setMacizoChartFocus(f: MacizoFocus | null) {
		if (JSON.stringify(f) === JSON.stringify(this.macizoChartFocus)) return;
		this.macizoChartFocus = f;
	}

	setColorVariable(v: string | null) {
		if (v === this.colorVariable) return;
		this.colorVariable = v;
		this.colorDomain = null;
		// Entries are memoized per data version: invalidate all three sets.
		this.dataVersion++;
		this.compareDataVersion++;
		this.regionalDataVersion++;
	}

	get effectivePrimary(): string {
		// Macizo layers have no parquet (loadedCols stays empty): the tab's variable is a tile property.
		if (this.colorVariable && (this.loadedCols.has(this.colorVariable) || this.activeLayer?.macizos)) return this.colorVariable;
		return this.primaryOverride ?? this.activeLayer?.primaryVariable ?? 'score';
	}

	// Classes for whatever the map paints: the colorVariable's own, else the layer's.
	get effectiveColorClasses(): ColorClasses | undefined {
		const layer = this.activeLayer;
		if (!layer) return undefined;
		// «Cambio» paints a signed change: only its own classes apply (censo_temporal), never the level's.
		if (layer.temporal && this.temporalMode === 'delta') return layer.deltaClasses?.[this.effectivePrimary];
		const c = layer.variableClasses?.[this.effectivePrimary] ?? layer.colorClasses;
		// A focused macizo reading lists only its own classes, plus the gray of the other stands.
		if (!c || !layer.macizos) return c;
		const pick = this.macizoChartFocus;
		if (pick) return { ...c, nominal: true, ordinal: false, zero: undefined, steps: [
			{ from: -2, color: HARVEST_HEX[3], labelKey: pick.label ?? '' },
			{ from: -1, color: NODATA_HEX, labelKey: 'harvest.otras' },
		] };
		const f = this.macizoFocus;
		if (!f) return c;
		return { ...c, zero: undefined, steps: [
			...c.steps.filter((s) => !f.sit || f.sit.includes(s.from)),
			{ from: -1, color: NODATA_HEX, labelKey: 'harvest.otras' },
		] };
	}

	private static PRIMARY_SKIP = new Set(['type', 'type_label', 'pca_1', 'pca_2', 'pca_3', 'territorial_type']);

	private updatePrimaryOverride(layer: HexLayerConfig, actualCols: Set<string>): void {
		if (this.activeLayer?.id !== layer.id) return; // stale async schema
		this.loadedCols = actualCols;
		if (actualCols.has(layer.primaryVariable)) {
			if (this.primaryOverride !== null) {
				this.primaryOverride = null;
				this.dataVersion++;
			}
			return;
		}
		const fallback = layer.variables
			.map(v => v.col)
			.find(c => !HexStore.PRIMARY_SKIP.has(c) && actualCols.has(c)) ?? null;
		if (fallback !== this.primaryOverride) {
			this.primaryOverride = fallback;
			this.dataVersion++; // repaint choropleth with the substituted primary
		}
	}

	setTerritoryPrefix(prefix: string) {
		if (this.territoryPrefix === prefix) return;
		this.territoryPrefix = prefix;
		this.deptFilter = null;
		this.deptFilterName = null;
		this.primaryOverride = null;
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
		this.analysisData = null;
		this.analysisResLevel = null;
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
		// Only sat_* parquets are territory-specific; EUDR etc. use a fixed global URL
		if (!this.territoryPrefix || !layer.parquet?.startsWith('sat_')) return url;
		return url.replace('/data/', `/data/${this.territoryPrefix}`);
	}

	/** Per-department parquet URL of another layer in the active territory (bivariate Y axis). */
	deptUrlForLayer(layerId: string, parquetKey: string, territoryPrefix = this.territoryPrefix): string | null {
		const layer = HEX_LAYER_REGISTRY[layerId];
		return layer ? this.deptUrlFor(layer, parquetKey, territoryPrefix) : null;
	}

	/** Per-department parquet URL for a layer (mirrors the dispatch in loadDepartment). */
	private deptUrlFor(layer: HexLayerConfig, parquetKey: string, territoryPrefix = this.territoryPrefix): string {
		if (layer.id === 'flood_risk') return getFloodDptoUrl(parquetKey, territoryPrefix);
		return getSatDptoUrl(layer.id, parquetKey, territoryPrefix);
	}

	/** Columns the dept UI actually consumes — used to project instead of SELECT *. */
	private async deptWantedCols(layer: HexLayerConfig, url: string): Promise<string[]> {
		const actualCols = await getParquetCols(url);
		this.updatePrimaryOverride(layer, actualCols);
		const wanted = ['h3index', layer.primaryVariable, 'type', 'type_label',
			'n_edificios', 'hogares_estimados',
			...layer.variables.flatMap(v => [v.col, v.rawCol, `${v.col}_raw`, v.baseCol, v.weightCol]),
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
		// AR dept polygons back the outline effects (which read a non-reactive module
		// var) and the hole-fill below — ensure they're loaded before selectedDpto flips.
		if (isArDeptTerritory(this.territoryPrefix)) await ensureArBoundaries();
		// PY/BR polygons too: the hexagons are cut at the official limit (Map setHexClip).
		else if (/_(py|br)\/$/.test(this.territoryPrefix)) await ensureBrBoundaries(this.territoryPrefix.slice(0, -1));
		this.selectedDpto = dpto;
		this.selectedParquetKey = parquetKey;
		this.visibleData = new Map();
		this.analysisData = null;
		this.analysisResLevel = null;
		this.clearSelection();
		this.stashZones();
		this.workDept = { prefix: this.territoryPrefix, name: dpto };
		this.chartSelection = null;
		this.clearCompareDept();
		this.clearRegionalData();
		// Layers drawn as DNDFI plantation polygons (forestry_harvest): no per-hex parquet. The department only
		// frames the map and filters the macizos; the panel reads the summary.
		if (layer.macizos) {
			const f = findDeptFeature(dpto, this.territoryPrefix);
			this.deptBbox = f ? HexStore.bboxOfGeometry(f.geometry) : null;
			this.loading = false;
			this.dataVersion++;
			return;
		}
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
					const an = analysisCache.get(cacheKey);
					if (an) this.analysisData = an;
					else void this.rebuildAnalysis(layer, url, cacheKey, deptCached.resLevel, deptCached.data);
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

			// Always project the full variable set (choropleth + panel + petal). In LOD
			// (coarse) mode these columns are averaged per coarse parent in the aggregation
			// loop below, so the petal/radar shows real data in the aggregated view too —
			// not only at res-9. (Was: LOD projected just the primary var → coarse hexes had
			// no covariates → empty petal / single point in the aggregated view.) The extra
			// numeric columns are cheap; the cost LOD avoids is the per-cell geometry build.
			const cols = await this.deptWantedCols(layer, url);
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
				// Class codes (land use, agri suitability): the coarse cell takes the most frequent class, never the mean
				// code (the mean of «monte» 1 and «agro» 4 would paint «perenne» 3).
				const nominalIdx = numericCols.map((c: string, k: number) => (HexStore.isClassCodeCol(layer, c) ? k : -1)).filter((k: number) => k >= 0);
				const modes = new Map<string, Map<number, number>[]>();
				const labelVec = resultCols.includes('type_label') ? result.getChild('type_label') : null;
				// Res-9 values for the charts (no geometry), unless the district is too big.
				const keepRes9 = result.numRows <= ANALYSIS_MAX_ROWS;
				const res9 = keepRes9 ? new Map<string, Record<string, any>>() : null;
				const sums = new Map<string, Float64Array>();
				const counts = new Map<string, Float64Array>();
				const labels = new Map<string, Map<string, number>>();
				// A census rate with a base column averages as Σ rate·base / Σ base in the coarse
				// cell (a plain mean weighs each radio by its count of cells); low_bld_pct, over
				// the cell's buildings (weightCol).
				const baseOf = numericCols.map((c: string) => {
					const vv = layer.variables.find(v => v.col === c);
					const bc = vv ? weightColOf(vv) : undefined;
					return bc ? numericCols.indexOf(bc) : -1;
				});
				for (let i = 0; i < result.numRows; i++) {
					let parent: string;
					const h3i = String(h3indexVec!.get(i));
					try { parent = cellToParent(h3i, coarseRes); } catch { continue; }
					if (res9) res9.set(h3i, HexStore.rowValues(resultCols, colVecs, i));
					let s = sums.get(parent);
					let c = counts.get(parent);
					if (!s || !c) { s = new Float64Array(numericCols.length); c = new Float64Array(numericCols.length); sums.set(parent, s); counts.set(parent, c); }
					for (let k = 0; k < numericCols.length; k++) {
						const v = colVecs[numericCols[k]]?.get(i);
						if (v === null || v === undefined) continue;
						const num = Number(v);
						if (!Number.isFinite(num)) continue;
						const b = baseOf[k];
						const w = b >= 0 ? Number(colVecs[numericCols[b]]?.get(i)) : 1;
						if (b >= 0 && !(w > 0)) continue;
						s[k] += num * w; c[k] += w;
					}
					if (nominalIdx.length) {
						let mm = modes.get(parent);
						if (!mm) { mm = nominalIdx.map(() => new Map<number, number>()); modes.set(parent, mm); }
						nominalIdx.forEach((k: number, j: number) => {
							const v = colVecs[numericCols[k]]?.get(i);
							if (v === null || v === undefined) return;
							const num = Number(v);
							if (Number.isFinite(num)) mm![j].set(num, (mm![j].get(num) ?? 0) + 1);
						});
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
					const mm = modes.get(parent);
					if (mm) nominalIdx.forEach((k: number, j: number) => {
						let best: number | null = null, bestN = -1;
						for (const [v, n] of mm[j]) if (n > bestN) { bestN = n; best = v; }
						if (best !== null) values[numericCols[k]] = best;
					});
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
				if (res9) { setAnalysisCache(cacheKey, res9); this.analysisData = res9; }
				else { this.analysisData = data; this.analysisResLevel = coarseRes; }
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
			// Not in PY: its district polygons now load for the map's clipping (Map setHexClip),
			// but the cells inside them that a district file lacks belong to the neighbour
			// district (the polygon and the partition disagree there): filled as "Sin
			// cobertura" they painted gray blocks inside Encarnación (1-nov-2026).
			const deptFeature = this.territoryPrefix.endsWith('_py/') ? null : findDeptFeature(dpto, this.territoryPrefix);
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

			// (Removed) The Misiones-only 2-ring "border buffer" grew gray nodata cells
			// around the whole dept perimeter to hide a thin dark sliver at the province
			// edge — but it also painted gray "sin cobertura" rings along every interior
			// department border, which read as missing data. Dropped: the only cost is a
			// ~1-hex dark sliver at the real province edge (which is the Paraná/Uruguay
			// river anyway). Bonus: skips a gridDisk×2 + cellToBoundary pass (faster load).

			// Persist computed geometry so re-visiting this dept is instant.
			setDeptCache(cacheKey, { data, centroids, boundaries, bbox: HexStore.bboxFromCentroids(centroids) });

			this.visibleData = data;
			this.centroidCache = centroids;
			this.analysisData = null;
			this.analysisResLevel = null;
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

	// One parquet row → plain values (numbers stay numbers, labels stay strings).
	/** Column of class codes, nominal (land use) or ordinal (agri suitability S1…N): aggregated by mode, not
	 *  mean — the mean of S1 and S2 cells (1,98) is no class, yet it painted and labelled «S1». */
	static isClassCodeCol(layer: HexLayerConfig | null | undefined, col: string): boolean {
		if (!layer) return false;
		const cls = layer.variableClasses?.[col] ?? (col === layer.primaryVariable ? layer.colorClasses : undefined);
		return !!(cls?.nominal || cls?.ordinal);
	}

	private static rowValues(cols: string[], vecs: Record<string, any>, i: number): Record<string, any> {
		const values: Record<string, any> = {};
		for (const col of cols) {
			const val = vecs[col]?.get(i);
			if (val === null || val === undefined) continue;
			const num = Number(val);
			values[col] = Number.isFinite(num) && typeof val !== 'string' ? num : String(val);
		}
		return values;
	}

	// Cached coarse overview whose res-9 analysis was evicted: re-read the values
	// (no geometry) and apply them only if the same department is still open.
	private async rebuildAnalysis(layer: HexLayerConfig, url: string, cacheKey: string, coarseRes: number, coarse: Map<string, Record<string, any>>): Promise<void> {
		const ctx = this.lodDeptContext;
		try {
			const cols = await this.deptWantedCols(layer, url);
			const result = await query(`SELECT ${cols.map(c => `"${c}"`).join(', ')} FROM '${url}'`);
			if (this.lodDeptContext !== ctx) return;
			if (result.numRows > ANALYSIS_MAX_ROWS) { this.analysisData = coarse; this.analysisResLevel = coarseRes; return; }
			const resultCols = result.schema.fields.map((f: any) => f.name).filter((n: string) => n !== 'h3index');
			const h3Vec = result.getChild('h3index');
			const colVecs = Object.fromEntries(resultCols.map((c: string) => [c, result.getChild(c)]));
			const res9 = new Map<string, Record<string, any>>();
			for (let i = 0; i < result.numRows; i++) res9.set(String(h3Vec!.get(i)), HexStore.rowValues(resultCols, colVecs, i));
			setAnalysisCache(cacheKey, res9);
			this.analysisData = res9;
		} catch (e) {
			console.warn('[rebuildAnalysis]', e);
		}
	}

	// Chart selections are res-9 cells; while the map draws a coarse overview,
	// paint the coarse cells that contain them.
	displayCells(h3s: string[]): string[] {
		const res = this.selectedHexResLevel;
		if (res == null || this.analysisResLevel != null) return h3s;
		const out = new Set<string>();
		for (const h of h3s) { try { out.add(cellToParent(h, res)); } catch { /* skip */ } }
		return [...out];
	}

	// Class maps (3×3, Moran quadrants) on the coarse overview: each coarse cell
	// takes the most frequent class among its res-9 children.
	displayEntries<T extends { h3index: string; value: number | null; nodata?: boolean }>(entries: T[]): { h3index: string; value: number | null; nodata?: boolean }[] {
		const res = this.selectedHexResLevel;
		if (res == null || this.analysisResLevel != null) return entries;
		const tally = new Map<string, Map<number, number>>();
		const seen = new Set<string>();
		for (const e of entries) {
			let parent: string;
			try { parent = cellToParent(e.h3index, res); } catch { continue; }
			seen.add(parent);
			if (e.nodata || e.value === null) continue;
			let t = tally.get(parent);
			if (!t) { t = new Map(); tally.set(parent, t); }
			t.set(e.value, (t.get(e.value) ?? 0) + 1);
		}
		const out: { h3index: string; value: number | null; nodata?: boolean }[] = [];
		for (const parent of seen) {
			const t = tally.get(parent);
			if (!t) { out.push({ h3index: parent, value: null, nodata: true }); continue; }
			let best = 0, bestN = -1;
			for (const [v, n] of t) if (n > bestN) { bestN = n; best = v; }
			out.push({ h3index: parent, value: best });
		}
		return out;
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

		// Load res-9 detail for the visible cells not loaded yet.
		let det = this.lodDetail;
		if (!det || det.ctx !== ctx) {
			det = this.lodDetail = { ctx, tried: new Set(), data: new Map(), centroids: new Map(), boundaries: new Map() };
		}
		const missing = cells.filter(c => !det!.tried.has(c));
		if (missing.length === 0) {
			if (this.selectedHexResLevel === null) return false; // already showing all of it
			this.showDetail(det);
			return true;
		}
		// Bound the accumulated detail: drop what is out of the current view first.
		if (det.data.size + missing.length > HexStore.DETAIL_KEEP) {
			const keep = new Set(cells);
			for (const h of [...det.tried]) if (!keep.has(h)) {
				det.tried.delete(h); det.data.delete(h); det.centroids.delete(h); det.boundaries.delete(h);
			}
		}
		try {
			const cols = await this.deptWantedCols(layer, ctx.url);
			const inList = missing.map(c => `'${c}'`).join(',');
			const result = await query(`SELECT ${cols.map(c => `"${c}"`).join(', ')} FROM '${ctx.url}' WHERE h3index IN (${inList})`);
			// Guard against a stale write: if a different dept/layer was selected while this
			// query was in flight, lodDeptContext changed — drop this result.
			if (this.lodDeptContext !== ctx || this.lodDetail !== det) return false;
			for (const c of missing) det.tried.add(c); // cells outside the dept are not re-asked
			const data = det.data, centroids = det.centroids, boundaries = det.boundaries;
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
			this.showDetail(det);
			return true;
		} catch (e) {
			console.warn('[loadDeptViewport]', e);
			return false;
		}
	}

	private static readonly DETAIL_KEEP = 24000;
	private showDetail(det: NonNullable<HexStore['lodDetail']>): void {
		// New Map instances: visibleData/caches are swapped by reference.
		this.visibleData = new Map(det.data);
		this.centroidCache = new Map(det.centroids);
		this.boundaryCache = new Map(det.boundaries);
		this.selectedHexResLevel = null; // real res-9 detail now → no "aggregated" badge
		this.dataVersion++;
	}

	async loadCompareDept(dpto: string, parquetKey: string, comparePrefix: string): Promise<void> {
		if (!this.activeLayer) return;
		const layer = this.activeLayer;
		this.loadError = null;
		this.compareGlobalPrefix = null; // dept-scoped compare → stop the viewport global refresh

		let url: string;
		if (layer.id === 'flood_risk') {
			url = getFloodDptoUrl(parquetKey, comparePrefix);
		} else {
			url = getSatDptoUrl(layer.id, parquetKey, comparePrefix);
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
		const url = getSatGlobalUrl(layer.id, prefix);
		// Project to only the columns the regional/compare UI needs (choropleth +
		// ComparisonPanel + petal + categorical) instead of SELECT * — the global carries
		// ~20 unused baseline/delta/pca columns. Inspect schema first to tolerate drift.
		const actualCols = await getParquetCols(url);
		const wanted = ['h3index', layer.primaryVariable, 'type', 'type_label',
			'n_edificios', 'hogares_estimados',
			...layer.variables.flatMap(v => [v.col, v.rawCol, `${v.col}_raw`, v.baseCol, v.weightCol]),
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
			this.analysisData = null;
			this.analysisResLevel = null;
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
		loadDeptSummary(layer.id, prefix).then(async summary => {
			if (this._prefetchToken !== token || !summary?.departments) return;
			// AR idle hole-fill (findDeptFeature) needs the dept polygons — warm them
			// before precomputing so cached depts aren't stored without the edge-fill.
			if (isArDeptTerritory(prefix)) { await ensureArBoundaries(); if (this._prefetchToken !== token) return; }
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
		else url = getSatDptoUrl(layer.id, key, prefix);

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

	/** Value of a raw parquet cell for the choropleth: NULL/NaN → null (painted «Sin dato»),
	 *  never 0, so a missing cell is not confused with a real 0 (reglamento §6). */
	private static entryValue(raw: unknown): number | null {
		if (raw === undefined || raw === null) return null;
		const v = Number(raw);
		return Number.isFinite(v) ? v : null;
	}

	/** Is the painted column a census rate (a variable with a baseCol)? See choroplethEntries_compute. */
	isCensusRate(pv: string): boolean {
		return !!this.activeLayer?.variables?.some((v: any) => (v.col === pv || v.rawCol === pv) && v.baseCol);
	}
	/** A census cell with no rate: off the census (no buildings) → not drawn; else a radio below MIN_BASE. */
	private static offCensus(data: Record<string, unknown>): boolean {
		return data.n_edificios != null && Number(data.n_edificios) === 0;
	}

	private regionalChoroplethEntries_compute(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] {
		if (!this.activeLayer) return [];
		const pv = this.effectivePrimary;
		const census = this.isCensusRate(pv);
		const entries: { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] = [];
		for (const [h3index, data] of this.regionalVisibleData) {
			const value = HexStore.entryValue(data[pv]);
			if (census && value === null && HexStore.offCensus(data)) continue;
			entries.push({ h3index, value, properties: data as Record<string, number>, boundary: this.regionalBoundaryCache.get(h3index), nodata: value === null, smallBase: census && value === null });
		}
		return entries;
	}

	get regionalChoroplethEntries(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] {
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
	private static bboxOfGeometry(g: any): [number, number, number, number] | null {
		let minLng = Infinity, minLat = Infinity, maxLng = -Infinity, maxLat = -Infinity;
		const walk = (c: any) => {
			if (typeof c[0] === 'number') {
				if (c[0] < minLng) minLng = c[0]; if (c[0] > maxLng) maxLng = c[0];
				if (c[1] < minLat) minLat = c[1]; if (c[1] > maxLat) maxLat = c[1];
			} else for (const x of c) walk(x);
		};
		if (g?.coordinates) walk(g.coordinates);
		return Number.isFinite(minLng) ? [minLng, minLat, maxLng, maxLat] : null;
	}

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

	private compareChoroplethEntries_compute(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] {
		if (!this.activeLayer) return [];
		const pv = this.effectivePrimary;
		const census = this.isCensusRate(pv);
		const entries: { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] = [];
		for (const [h3index, data] of this.compareVisibleData) {
			const value = HexStore.entryValue(data[pv]);
			if (census && value === null && HexStore.offCensus(data)) continue;
			entries.push({ h3index, value, properties: data as Record<string, number>, boundary: this.compareBoundaryCache.get(h3index), nodata: value === null, smallBase: census && value === null });
		}
		return entries;
	}

	get compareChoroplethEntries(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] {
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
		this.stashZones();
		// «Volver a los departamentos» is the explicit way back to the ranking: the next layer opens there.
		this.workDept = null;
	}

	async loadVisibleData() {
		const layer = this.activeLayer;
		if (!layer || !isReady()) return;
		if (layer.perDepartment) return;

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

		// Project only columns the parquet actually has — config drift on a single
		// declared column must not Binder-error the whole layer load.
		const actualCols = await getParquetCols(url);
		this.updatePrimaryOverride(layer, actualCols);
		const baseCols = layer.variables.map(v => v.col);
		const allCols = new Set(baseCols);
		if (layer.temporal) {
			for (const col of baseCols) {
				allCols.add(getTemporalCol(col, 'baseline'));
				allCols.add(getTemporalCol(col, 'delta'));
			}
		}
		for (const col of [...allCols]) {
			if (!actualCols.has(col)) allCols.delete(col);
		}
		const cols = [...allCols].join(', ');
		const result = await query(
			`SELECT h3index${cols ? `, ${cols}` : ''} FROM '${url}'`
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
		this.analysisData = null;
		this.analysisResLevel = null;
		this.boundaryCache = boundaries;
		this.dataVersion++;

		// Persist in module-level cache for instant re-activation
		layerDataCache.set(layer.id, { data, centroids, boundaries, provincialAvg: null });
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

	// ── Clicked selection across layers (8-nov-2026) ─────────────────────
	// Radios/cells clicked in a department stay picked when the layer or the lens changes:
	// setLayer/clearAll put them aside with the department, and syncSelection gives them back
	// with the new layer's rows once that department's cells are in. Another department, or
	// the page reset (Escape), drops them. Kept in memory only (zones are what gets saved).
	private selectionCarry: { key: string; hexes: [string, string][]; colorIndex: number } | null = null;

	private stashSelection() {
		const key = this.zoneDeptKey();
		if (this.selectedHexes.size && key) {
			this.selectionCarry = { key, hexes: [...this.selectedHexes].map(([h, s]) => [h, s.color]), colorIndex: this.colorIndex };
		}
		this.selectedHexes = new Map();
		this.colorIndex = 0;
	}

	dropSelectionCarry() {
		this.selectionCarry = null;
	}

	/** True when the carried selection was put back (the caller repaints nothing: the map
	 *  highlight follows selectedHexes on its own). */
	syncSelection(): boolean {
		const c = this.selectionCarry;
		const key = this.zoneDeptKey();
		if (!c || !key || !this.activeLayer) return false;
		if (c.key !== key) { this.selectionCarry = null; return false; }
		if (this.chartData.size === 0 || (this.lodDeptActive && !this.analysisData)) return false;
		this.selectionCarry = null;
		if (this.selectedHexes.size) return false;   // already picking in the new layer
		const m = new Map<string, HexSelectionData>();
		for (const [h, color] of c.hexes) m.set(h, { color, data: this.zoneRow(h) ?? {} });
		this.selectedHexes = m;
		this.colorIndex = c.colorIndex;
		return true;
	}

	// ── Choropleth entries ────────────────────────────────────────────────

	// Private compute method (for type inference in memoization above)
	private choroplethEntries_compute() {
		if (!this.activeLayer) return [];
		const effectivePrimary = this.activeLayer.temporal && this.temporalMode !== 'current'
			? getTemporalCol(this.effectivePrimary, this.temporalMode)
			: this.effectivePrimary;
		const isDelta = this.activeLayer.temporal && this.temporalMode === 'delta';
		// Diverging delta ramp is tierra(−)→río(+), i.e. assumes higher = better. For
		// "danger" layers (colorScale 'flood', higher = worse: deforestation, PM2.5) an
		// INCREASE must read tierra (worse), not río. Flip the delta sign so the ramp + the
		// worse/better legend labels stay correct. Layers where higher = better/neutral
		// (carbon, censo density: sequential) keep the raw sign.
		const invertDelta = isDelta && this.activeLayer.colorScale === 'flood';
		const entries: { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] = [];
		const deptOnly = this.deptFilter;
		// A census rate (variable with a baseCol) has no value in two cases (5-nov-2026):
		// the cell has no buildings, so it is not in the census at all (economic_activity
		// ships the full night-lights grid: 80 % of Capital was painted «sin cobertura»),
		// and is not drawn; or its radio has fewer than MIN_BASE (30) in the base, so no
		// rate is published: smallBase, hatched on the map with its own legend row.
		const pv = this.effectivePrimary;
		const censusRate = !isDelta && this.isCensusRate(pv);
		for (const [h3index, data] of this.visibleData) {
			if (deptOnly && !deptOnly.has(h3index)) continue;
			const raw = data[effectivePrimary];
			const hasData = raw !== undefined && raw !== null &&
				(typeof raw !== 'number' || Number.isFinite(raw));
			if (censusRate && !hasData && HexStore.offCensus(data)) continue;
			let value: number | null = hasData ? Number(raw) : null;
			if (invertDelta && value !== null) value = -value;
			if (isDelta && hasData && value === 0) continue;
			entries.push({
				h3index,
				value,
				properties: data,
				boundary: this.boundaryCache.get(h3index),
				nodata: !hasData,
				smallBase: censusRate && !hasData
			});
		}
		return entries;
	}

	get choroplethEntries(): { h3index: string; value: number | null; properties: Record<string, number>; boundary?: number[][]; nodata?: boolean; smallBase?: boolean }[] {
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
				// A rate over a base (households, people) averages as Σ rate·base / Σ base, the
				// department ranking's rule (build_layer_ranks.py): a plain AVG over cells weighs
				// each census radio by its number of cells, not by its households.
				const aggExprs = availableVars.map(v => {
					const wc = weightColOf(v);
					return wc && actualCols.has(wc)
						? `SUM(${v.col} * ${wc}) / NULLIF(SUM(CASE WHEN ${v.col} IS NOT NULL THEN ${wc} END), 0) as avg_${v.col}`
						: `AVG(${v.col}) as avg_${v.col}`;
				}).join(', ');
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

		// Two-scale layers: frozen per-territory P2–P98 (provincial) or the fixed
		// regional range. No stats for this territory → fall through to MIN/MAX.
		if (layer.regionalColorDomain) {
			if (this.scaleMode === 'regional' || this.temporalMode === 'baseline') {
				this.colorDomain = layer.regionalColorDomain;
				return this.colorDomain;
			}
			const stats = layer.scaleStats?.territories[this.territoryId];
			if (stats) {
				this.colorDomain = [stats.p2, stats.p98];
				return this.colorDomain;
			}
		}

		const dataUrl = this.layerGlobalUrl(layer);
		if (!dataUrl) return null;

		try {
			// Resolve the per-territory primary fallback BEFORE querying: MIN/MAX on a
			// column the parquet lacks is a Binder error (a territory published without the
			// primary column). getParquetCols caches, so this is one-time per URL.
			this.updatePrimaryOverride(layer, await getParquetCols(dataUrl));
			const pv = this.effectivePrimary;
			// No WHERE … IS NOT NULL: MIN/MAX already ignore NULLs, and the predicate
			// blocks DuckDB's footer-statistics pushdown (forces a full column scan).
			// Census rates are skewed: P2–P98 keeps the map from washing out (the
			// census parquets are small, so the column scan is cheap).
			const sql = layer.quantileDomain
				? `SELECT quantile_cont(${pv}, 0.02) as lo, quantile_cont(${pv}, 0.98) as hi FROM '${dataUrl}'`
				: `SELECT MIN(${pv}) as lo, MAX(${pv}) as hi FROM '${dataUrl}'`;
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

	/** Row of a zone cell: res-9 chart cells live in chartData (analysis data, also
	 *  under the coarse overview); lasso cells are whatever the map draws. */
	zoneRow(h3: string): Record<string, any> | undefined {
		return this.chartData.get(h3) ?? this.visibleData.get(h3);
	}

	async createHexZone(h3indices: string[], polygon: [number, number][] | null, origin: string,
		opts: { id?: string; sharedN?: number; outline?: string } = {}): Promise<string | null> {
		if (h3indices.length === 0 || !this.activeLayer) return null;
		if (!isReady()) return null;

		// First free letter among all saved zones (every department), so removing A and adding
		// again never repeats B; a zone from a link keeps its own letter («zona C» in a post stays C).
		const dept = this.zoneScopeKey();
		const used = new Set(this.savedZones.map(z => z.id));
		const asked = opts.id && !used.has(opts.id) ? ZONE_LABELS.indexOf(opts.id) : -1;
		const idx = asked >= 0 ? asked : ZONE_LABELS.findIndex(l => !used.has(l));
		const id = idx >= 0 ? ZONE_LABELS[idx] : `Z${this.savedZones.length + 1}`;
		// Colour: the first one no zone of this department wears (they are compared side by side).
		const deptColors = new Set(this.savedZones.filter(z => z.dept === dept).map(z => z.color));
		const color = ZONE_COLORS.find(c => !deptColors.has(c))
			?? ZONE_COLORS[this.savedZones.filter(z => z.dept === dept).length % ZONE_COLORS.length];

		try {
			const stats = await this.computeZoneStats(h3indices);
			const zone: HexZone = {
				id,
				color,
				h3indices,
				polygon,
				origin,
				stats,
				dept,
				layerId: this.activeLayer.id,
				// Computed now, while the cells it was drawn on are the ones in centroidCache.
				outline: polygon ? (opts.outline ?? urlOutline(polygon, h3indices, (r) => this.findHexesInPolygon(r))) : null,
				...(opts.sharedN !== undefined ? { sharedN: opts.sharedN } : {}),
			};

			this.hexZones = [...this.hexZones, zone];
			this.savedZones = [...this.savedZones, zone];
			this.persistZones();
			this.zonesCreated++;
			return id;
		} catch (e) {
			console.warn('Failed to create hex zone:', e);
			return null;
		}
	}

	/** Mean of each numeric variable of the active layer over the zone's cells
	 *  (utils/zoneStats.aggregateRows: the same rule as the department reference). */
	private async computeZoneStats(h3indices: string[]): Promise<HexZoneStats> {
		const provAvg = await this.ensureProvincialAvg();
		const agg = aggregateRows(h3indices.map(h => this.zoneRow(h)), this.numericVariables);
		return {
			hexCount: h3indices.length,
			cellsWithData: agg.cellsWithData,
			rawValues: agg.rawValues,
			totals: agg.totals,
			normalizedValues: this.normalize(agg.rawValues, provAvg),
		};
	}

	/** The open department (the cells the charts read: chartData) aggregated with the zones'
	 *  rule, so each zone value reads next to its department's. Memoized per data snapshot. */
	private _deptRef: { src: Map<string, Record<string, any>>; layerId: string; agg: ReturnType<typeof aggregateRows> } | null = null;
	get zoneDeptRef(): ReturnType<typeof aggregateRows> | null {
		const layer = this.activeLayer;
		const src = this.chartData;
		if (!layer || src.size === 0) return null;
		const c = this._deptRef;
		if (c && c.src === src && c.layerId === layer.id) return c.agg;
		const agg = aggregateRows(src.values(), this.numericVariables);
		this._deptRef = { src, layerId: layer.id, agg };
		return agg;
	}

	// ── Saved zones ──────────────────────────────────────────────────────
	// A zone is drawn once and kept (savedZones) until the user removes it: across
	// layers, lenses, departments and a reload of the tab (sessionStorage). hexZones
	// holds the ones of the open department, recomputed with the active layer by
	// syncZones() once that layer has the department's cells. The others are listed
	// in the panel («Otras zonas guardadas») with a way back to them (8-nov-2026).
	savedZones: HexZone[] = $state.raw([]);
	private _zoneSyncBusy = false;
	/** Zones made by createHexZone (lasso, chart pick, shared link); syncZones' restore does
	 *  not count. The panel opens the zones block on a new zone, not on a layer change. */
	zonesCreated = $state(0);
	/** The department the user is working in, kept across layer changes: a new layer opens
	 *  there instead of its ranking. Cleared by «Volver a los departamentos» and the full reset. */
	workDept: { prefix: string; name: string } | null = $state(null);
	get workDeptKey(): string | null {
		return this.workDept ? `${this.workDept.prefix}|${this.workDept.name}` : null;
	}

	/** Saved zones outside the open department, grouped by department (panel list). */
	get otherZoneGroups(): { key: string; prefix: string; name: string; zones: HexZone[] }[] {
		const open = this.zoneScopeKey();
		const groups = new Map<string, HexZone[]>();
		for (const z of this.savedZones) {
			if (!z.dept || z.dept === open) continue;
			const g = groups.get(z.dept);
			if (g) g.push(z); else groups.set(z.dept, [z]);
		}
		return [...groups.entries()].map(([key, zones]) => {
			const i = key.indexOf('|');
			return { key, prefix: key.slice(0, i), name: key.slice(i + 1), zones };
		});
	}

	/** Zones the URL carries: the ones on screen, or the saved ones of the working department
	 *  while the new layer loads (a layer change must not drop `z` from the link). Lasso zones only. */
	get urlZones(): HexZone[] {
		const key = this.zoneScopeKey() ?? this.workDeptKey;
		const zs = this.hexZones.length ? this.hexZones : this.savedZones.filter(z => z.dept === key);
		return zs.filter(z => z.outline);
	}

	/** Zones from a shared link, drawn once the link's department has its cells (syncZones). */
	pendingUrlZones: { dept: string; zones: UrlZone[]; names?: Map<string, string> } | null = null;
	/** Zone scope (zoneScopeKey) whose zones the map frames once they are drawn: a shared
	 *  link, «ir» on a saved zone. Null = leave the camera alone. */
	flyToZonesKey: string | null = $state(null);
	private static normName(s: string): string {
		return s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
	}

	private async restoreUrlZones(key: string): Promise<boolean> {
		const p = this.pendingUrlZones;
		if (!p) return false;
		const name = key.slice(key.indexOf('|') + 1);
		// Another department opened before the link's one finished loading: drop them.
		this.pendingUrlZones = null;
		if (HexStore.normName(name) !== HexStore.normName(p.dept)) return false;
		let any = false;
		for (const z of p.zones) {
			const linkName = p.names?.get(z.id);
			// A reload carries in ?z= the zones this tab already saved: draw each one once
			// (and still frame them, as any link does). The link's name wins over none.
			const saved = this.savedZones.find(s => s.dept === key && s.outline === z.outline);
			if (saved) {
				this.flyToZonesKey = key;
				if (linkName && !saved.name) this.renameHexZone(saved.id, linkName);
				continue;
			}
			const cells = this.findHexesInPolygon(z.ring);
			if (cells.length === 0) continue;
			const id = await this.createHexZone(cells, z.ring, i18n.t('zone.origin.lasso'), { id: z.id, sharedN: z.n, outline: z.outline });
			if (id && linkName) this.renameHexZone(id, linkName);
			any = any || id !== null;
		}
		if (any) this.flyToZonesKey = key;
		return any;
	}

	/** Territory + department the user has open, in any layer (per-department
	 *  parquet or a territory-wide layer narrowed with its own list). */
	zoneDeptKey(): string | null {
		const name = this.selectedDpto ?? this.deptFilterName;
		return name ? `${this.territoryPrefix}|${name}` : null;
	}

	/** Where a zone belongs: the open department, or the whole territory on a territory-wide
	 *  layer (censo_temporal) with no department picked (`prefix|`, empty name). */
	zoneScopeKey(): string | null {
		const d = this.zoneDeptKey();
		if (d) return d;
		return this.activeLayer && !this.activeLayer.perDepartment ? `${this.territoryPrefix}|` : null;
	}

	/** Layer, lens or department changes: the zones leave the screen, they stay saved. */
	private stashZones() {
		this.hexZones = [];
	}

	/** Bring back the saved zones of the open department and recompute every one whose
	 *  stats belong to another layer. True when the zones changed (repaint them). */
	async syncZones(): Promise<boolean> {
		const layer = this.activeLayer;
		const key = this.zoneScopeKey();
		if (!layer || !key || this._zoneSyncBusy) return false;
		// The department's cells of THIS layer: none yet while it loads, and a LOD
		// department reads its res-9 cells from analysisData, not the coarse overview.
		if (this.chartData.size === 0 || (this.lodDeptActive && !this.analysisData)) return false;
		this._zoneSyncBusy = true;
		try {
			let changed = false;
			const mine = this.savedZones.filter(z => z.dept === key);
			const shown = new Set(this.hexZones.map(z => z.id));
			if (mine.some(z => !shown.has(z.id)) || this.hexZones.some(z => z.dept !== key)) {
				this.hexZones = mine;
				changed = true;
			}
			// Saved first, then the link: a link zone already saved is not drawn twice.
			if (this.pendingUrlZones && await this.restoreUrlZones(key)) changed = true;
			const stale = this.hexZones.filter(z => z.layerId !== layer.id);
			if (stale.length === 0) return changed;
			const fresh = new Map<string, HexZoneStats>();
			for (const z of stale) {
				const s = await this.computeZoneStats(z.h3indices);
				// No cell of the zone found yet (data still arriving): try again on the next load.
				if (s.cellsWithData === 0 && this.chartData.size < z.h3indices.length) continue;
				fresh.set(z.id, s);
			}
			if (this.activeLayer !== layer || fresh.size === 0) return changed;
			const upd = (z: HexZone) => {
				const s = fresh.get(z.id);
				return s && z.dept === key ? { ...z, stats: s, layerId: layer.id } : z;
			};
			this.hexZones = this.hexZones.map(upd);
			this.savedZones = this.savedZones.map(upd);
			return true;
		} finally {
			this._zoneSyncBusy = false;
		}
	}

	removeHexZone(id: string) {
		this.hexZones = this.hexZones.filter(z => z.id !== id);
		this.savedZones = this.savedZones.filter(z => z.id !== id);
		this.persistZones();
	}

	/** The user's «Limpiar zonas»: the open department's zones, gone for good. */
	clearHexZones() {
		const key = this.zoneScopeKey();
		const ids = new Set(this.hexZones.map(z => z.id));
		this.hexZones = [];
		this.savedZones = this.savedZones.filter(z => !ids.has(z.id) && (key === null || z.dept !== key));
		this.persistZones();
	}

	/** ✎ on a zone: its name, or none (back to the letter alone). */
	renameHexZone(id: string, name: string) {
		const n = cleanZoneName(name);
		const upd = (z: HexZone): HexZone => {
			if (z.id !== id) return z;
			const { name: _old, ...rest } = z;
			return n ? { ...rest, name: n } : rest;
		};
		this.hexZones = this.hexZones.map(upd);
		this.savedZones = this.savedZones.map(upd);
		this.persistZones();
	}

	/** ✕ on a department of «Otras zonas guardadas». */
	clearSavedZones(deptKey: string) {
		this.savedZones = this.savedZones.filter(z => z.dept !== deptKey);
		this.hexZones = this.hexZones.filter(z => z.dept !== deptKey);
		this.persistZones();
	}

	/** Saved zones → sessionStorage. Stats are recomputed on the way back (layerId ''). */
	private persistZones() {
		try {
			if (typeof sessionStorage === 'undefined') return;
			if (this.savedZones.length === 0) { sessionStorage.removeItem(ZONES_STORAGE_KEY); return; }
			sessionStorage.setItem(ZONES_STORAGE_KEY, JSON.stringify(this.savedZones.map(z => ({
				id: z.id, color: z.color, h3indices: z.h3indices, polygon: z.polygon, origin: z.origin,
				dept: z.dept, outline: z.outline, ...(z.sharedN !== undefined ? { sharedN: z.sharedN } : {}),
				...(z.name ? { name: z.name } : {}),
			}))));
		} catch { /* storage full or blocked: the zones live until the tab reloads */ }
	}

	/** The tab's saved zones, at page start. */
	loadSavedZones() {
		try {
			if (typeof sessionStorage === 'undefined') return;
			const raw = sessionStorage.getItem(ZONES_STORAGE_KEY);
			if (!raw) return;
			const arr = JSON.parse(raw);
			if (!Array.isArray(arr)) return;
			this.savedZones = arr
				.filter((z: any) => z && typeof z.id === 'string' && Array.isArray(z.h3indices) && z.h3indices.length && typeof z.dept === 'string')
				.map((z: any): HexZone => ({
					id: z.id, color: z.color, h3indices: z.h3indices, polygon: z.polygon ?? null,
					origin: z.origin ?? '', dept: z.dept, outline: z.outline ?? null, layerId: '',
					stats: { hexCount: z.h3indices.length, cellsWithData: 0, rawValues: [], totals: [], normalizedValues: [] },
					...(typeof z.sharedN === 'number' ? { sharedN: z.sharedN } : {}),
					...(typeof z.name === 'string' && cleanZoneName(z.name) ? { name: cleanZoneName(z.name) } : {}),
				}));
		} catch { /* unreadable: start without zones */ }
	}

	// ── Petal chart data ─────────────────────────────────────────────────

	get petalLayers(): Array<{ values: number[]; color: string }> {
		// Only zones already recomputed with the active layer (a carried zone holds the old one's).
		return this.hexZones.filter(z => z.layerId === this.activeLayer?.id).map(z => ({
			values: z.stats.normalizedValues,
			color: z.color,
		}));
	}

	// Indexes (into numericVariables) of the flower's petals: all but noPetal variables.
	private get petalIdx(): number[] {
		return this.numericVariables.flatMap((v, i) => (v.noPetal ? [] : [i]));
	}

	get petalLabels(): string[] {
		const vars = this.numericVariables;
		return this.petalIdx.map(i => i18n.t(vars[i].labelKey));
	}

	// ── Selection petal data (individual hex clicks) ─────────────────────

	get selectionPetalLayers(): Array<{ values: number[]; color: string }> {
		return this.petalRose([...this.selectedHexes.values()]).layers;
	}

	/**
	 * The flower for arbitrary rows (a cell's data, a census radio's row), each normalized
	 * against the provincial average; `keep` narrows the petals (radio mode keeps only the
	 * radio's variables). Empty until the provincial average is loaded.
	 */
	petalRose(
		rows: Array<{ data: Record<string, any>; color: string }>,
		keep: (v: HexVariable) => boolean = () => true,
	): { labels: string[]; layers: Array<{ values: number[]; color: string }> } {
		const vars = this.numericVariables;
		const idx = this.petalIdx.filter(i => keep(vars[i]));
		const labels = idx.map(i => i18n.t(vars[i].labelKey));
		if (!this.activeLayer || rows.length === 0 || !this.provincialAvg) return { labels, layers: [] };
		const provAvg = this.provincialAvg;
		const layers = rows.map(r => {
			const normalizedValues = this.normalize(vars.map(v => r.data[v.col] ?? 0), provAvg);
			return { values: idx.map(i => normalizedValues[i]), color: r.color };
		});
		return { labels, layers };
	}

	async ensureProvincialAvgLoaded(): Promise<void> {
		await this.ensureProvincialAvg();
	}

	// ── Full clear ───────────────────────────────────────────────────────

	clearLoadError() {
		this.loadError = null;
	}

	clearAll() {
		// A lens change goes through here: the clicked selection is kept aside, like in setLayer.
		this.stashSelection();
		this.activeLayer = null;
		this.visibleData = new Map();
		this.centroidCache = new Map();
		this.analysisData = null;
		this.analysisResLevel = null;
		this.boundaryCache = new Map();
		this.selectedDpto = null;
		this.selectedParquetKey = null;
		this.deptBbox = null;
		this.loadError = null;
		this.temporalMode = 'current';
		this.dataVersion++;
		this.selectedHexes = new Map();
		this.stashZones();
		this.chartSelection = null;
		this.colorIndex = 0;
		this.provincialAvg = null;
		this.colorDomain = null;
		this.clearCompareDept();
		this.clearRegionalData();
	}
}
