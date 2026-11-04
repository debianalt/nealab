import { COLOR_RAMPS } from '$lib/config';
import { untrack } from 'svelte';
import { themeStore } from '$lib/stores/theme.svelte';
import { BUILDING_VOLUME_CLARO } from '$lib/utils/sello';

// Untracked: these expressions are built inside territory $effects (+page, Map); a
// tracked read re-ran them on every theme switch and closed the open department
// (found in prod 1-nov-2026). Map.applyOwnLayersTheme repaints the buildings itself.
const claroNow = () => untrack(() => themeStore.tema === 'claro');
// Same breaks, Claro colors (sello.ts): stops [b0, c0, b1, c1, ...] with c swapped.
const claroStops = (stops: readonly (number | string)[]) =>
	stops.map((v, i) => (i % 2 === 1 ? BUILDING_VOLUME_CLARO[(i - 1) / 2] ?? v : v));

export type RadioData = {
	color: string;
	census: Record<string, any>;
	enriched: Record<string, any> | null;
	buildingCount: number;
};

const RADIO_COLORS = ['#60a5fa', '#f97316', '#22c55e', '#a855f7', '#ef4444', '#eab308'];

export type HexData = {
	h3index: string;
	jrc_occurrence?: number;
	jrc_recurrence?: number;
	jrc_seasonality?: number;
	flood_extent_pct?: number;
};

export type DistrictData = {
	color: string;
	personas: number;
	territory: string;   // itapua_py | alto_parana_py — for per-territory highlight + enrichment parquet
	enriched: Record<string, any> | null;
};


export class MapStore {
	selectedRadios: Map<string, RadioData> = $state(new Map());
	selectedDistricts: Map<string, DistrictData> = $state(new Map());
	pitch: number = $state(31);
	bearing: number = $state(-15);
	activeHexLayer: string | null = $state(null);
	// Hexagon fill opacity, user-controlled via the bottom-left slider so the basemap
	// (streets / buildings / satellite) shows through for orientation. 0.78 = original default.
	hexOpacity: number = $state(0.78);
	// Basemap reference mode: 'map' = vector basemap (Claro or Oscuro), 'satellite' =
	// imagery underlay. Derived from themeStore.fondo, the one source of truth.
	get basemapMode(): 'map' | 'satellite' { return themeStore.fondo === 'satelite' ? 'satellite' : 'map'; }
	// Forestry plantations overlay (DNDFI): controlled from the right panel
	// (AnalysisView), only when forestry_aptitude is active with a dept selected.
	// plantationsDept = the selected department name, used to filter the overlay to
	// that department (loads per-dept, not by zoom).
	plantationsVisible: boolean = $state(false);
	plantationsDept: string | null = $state(null);
	selectedHex: HexData | null = $state(null);
	private colorIndex = 0;
	private districtColorIndex = 0;

	get currentRamp() {
		return COLOR_RAMPS.volume;
	}

	getColorExpr(): unknown[] {
		const ramp = this.currentRamp;
		// Color buildings by built volume = footprint area × height (m³). Direct GBA
		// measurement, identical method across all territories → objectively comparable,
		// no census-allocation artifact. Coalesced low-zoom BR blobs carry summed area +
		// mean height, so volume stays ~correct at every zoom. est_personas → click only.
		// to-number: the BR tiles carry area_m2 as a string, which made '*' fail and
		// painted every Brazilian building black (fallback colour; fixed 1-nov-2026).
		const value = ['*', ['to-number', ['get', 'area_m2'], 0], ['to-number', ['coalesce', ['get', 'best_height_m'], 3], 3]];
		return [
			'interpolate', ['linear'],
			value,
			...(claroNow() ? claroStops(ramp.stops) : ramp.stops)
		];
	}

	getHeightColorExpr(): unknown[] {
		const ramp = COLOR_RAMPS.height;
		return [
			'interpolate', ['linear'],
			['coalesce', ['get', ramp.property], 3],
			...(claroNow() ? claroStops(ramp.stops) : ramp.stops)
		];
	}

	addRadio(redcode: string, data: Omit<RadioData, 'color'>, colorOverride?: string) {
		if (this.selectedRadios.has(redcode)) return;
		const color = colorOverride || RADIO_COLORS[this.colorIndex % RADIO_COLORS.length];
		if (!colorOverride) this.colorIndex++;
		const updated = new Map(this.selectedRadios);
		updated.set(redcode, { ...data, color });
		this.selectedRadios = updated;
	}

	removeRadio(redcode: string) {
		if (!this.selectedRadios.has(redcode)) return;
		const updated = new Map(this.selectedRadios);
		updated.delete(redcode);
		this.selectedRadios = updated;
		if (updated.size === 0) this.colorIndex = 0;
	}

	hasRadio(redcode: string): boolean {
		return this.selectedRadios.has(redcode);
	}

	clearRadios() {
		this.selectedRadios = new Map();
		this.colorIndex = 0;
	}

	// Keyed by distrito name: Itapúa and Alto Paraná district names are
	// disjoint, so name collisions across the 2 PY territories don't occur.
	// territory is stored per entry for per-territory highlight + the
	// correct district_stats parquet on (re)render.
	addDistrict(distrito: string, personas: number, territory: string = 'itapua_py', colorOverride?: string) {
		if (this.selectedDistricts.has(distrito)) return;
		const color = colorOverride || RADIO_COLORS[this.districtColorIndex % RADIO_COLORS.length];
		if (!colorOverride) this.districtColorIndex++;
		const updated = new Map(this.selectedDistricts);
		updated.set(distrito, { color, personas, territory, enriched: null });
		this.selectedDistricts = updated;
	}

	removeDistrict(distrito: string) {
		const updated = new Map(this.selectedDistricts);
		updated.delete(distrito);
		this.selectedDistricts = updated;
		if (updated.size === 0) this.districtColorIndex = 0;
	}

	hasDistrict(d: string): boolean {
		return this.selectedDistricts.has(d);
	}

	clearDistricts() {
		this.selectedDistricts = new Map();
		this.districtColorIndex = 0;
	}

	updateDistrictEnriched(distrito: string, enriched: Record<string, any>) {
		const existing = this.selectedDistricts.get(distrito);
		if (!existing) return;
		const updated = new Map(this.selectedDistricts);
		updated.set(distrito, { ...existing, enriched });
		this.selectedDistricts = updated;
	}

	updateEnriched(redcode: string, enriched: Record<string, any>) {
		const existing = this.selectedRadios.get(redcode);
		if (!existing) return;
		const updated = new Map(this.selectedRadios);
		updated.set(redcode, { ...existing, enriched });
		this.selectedRadios = updated;
	}

	updateCensus(redcode: string, census: Record<string, any>) {
		const existing = this.selectedRadios.get(redcode);
		if (!existing) return;
		const updated = new Map(this.selectedRadios);
		updated.set(redcode, { ...existing, census: { ...existing.census, ...census } });
		this.selectedRadios = updated;
	}

	setActiveHexLayer(layer: string | null) {
		this.activeHexLayer = layer;
	}

	setSelectedHex(hex: HexData | null) {
		this.selectedHex = hex;
	}

	clearHexState() {
		this.activeHexLayer = null;
		this.selectedHex = null;
	}
}
