// Sello de Spatia: the data colors and type as tokens.
//
// SVG marks use the CSS variables (src/app.css), so print swaps them to the light
// variants on its own. MapLibre paint properties cannot read CSS variables: map
// code uses SELLO_HEX (screen/dark values). Keep both in sync.
//
// Roles: categorical in fixed order (1 tierra · 2 río · 3 selva · 4 yerba-oro,
// never cycled); sequential tierra for magnitude; diverging río · gris · tierra
// for change (río = better/lower, tierra = worse/higher). Text never wears a
// series color: it uses INK.

export const CAT = ['var(--s-cat-1)', 'var(--s-cat-2)', 'var(--s-cat-3)', 'var(--s-cat-4)'] as const;
export const TIERRA = CAT[0];
export const RIO = CAT[1];
export const SELVA = CAT[2];
export const ORO = CAT[3];

export const SEQ = ['var(--s-seq-1)', 'var(--s-seq-2)', 'var(--s-seq-3)', 'var(--s-seq-4)', 'var(--s-seq-5)'] as const;
export const DIV = [
	'var(--s-div-1)', 'var(--s-div-2)', 'var(--s-div-3)', 'var(--s-div-4)',
	'var(--s-div-5)', 'var(--s-div-6)', 'var(--s-div-7)',
] as const;

export const INK_1 = 'var(--s-ink-1)';
export const INK_2 = 'var(--s-ink-2)';
export const INK_3 = 'var(--s-ink-3)';
export const GRID = 'var(--s-grid)';
export const AXIS = 'var(--s-axis)';
// What the reader picked (chart brush): tierra in the
// chart and a tierra tint on the map.
export const SELECT = TIERRA;
// Outline of anything selected on the map (click, lasso, chart, compare, census
// radios): white over the dark basemap. A light basemap will need a dark one.
export const SELECT_OUTLINE = '#ffffff';
export const SURFACE = 'var(--s-surface)';

// ── Map colors that depend on the basemap, per theme ───────────────────────────
// Oscuro = the values the map has always had (vitrina, and Satélite). Claro = the
// atlas sheet of the El Soberbio demo (demo/el-nino: atlasStyle, TH): paper, gray
// shaded relief under the roads, administrative limits as a dashed gray line,
// selection in ink. Map.svelte applyMapTheme() sets these on the mounted layers.
export type MapTema = 'claro' | 'oscuro';
export interface MapThemeColors {
	paper: string | null;          // basemap background (null = the style's own)
	water: string | null;
	waterway: string | null;
	placeText: string | null;      // place-name labels
	halo: string; haloWidth: number;
	mask: string; maskOpacity: number;           // outside the active territory
	territoryBg: string; territoryBgOpacity: number; // under the hexagons (no-parquet cells)
	border: string; borderDash: number[] | null; // the 24 territory limits
	regionOutline: string;          // start view: outline of the whole area
	province: string;               // census radio borders (province context)
	deptLine: string;               // department limits (pickers)
	accent: string;                 // selected department, picker tint
	compare: string;                // compared department
	select: string;                 // outline of anything selected
	hillshade: { shadow: string; highlight: string; accent: string | undefined; exaggeration: number };
	light: { color: string; intensity: number };
}
export const MAP_THEME: Record<MapTema, MapThemeColors> = {
	oscuro: {
		paper: null, water: null, waterway: null, placeText: null,
		halo: 'rgba(8,10,14,0.9)', haloWidth: 1.4,
		mask: '#1a1a2e', maskOpacity: 0.75,
		territoryBg: 'rgb(55,65,81)', territoryBgOpacity: 0.65,
		border: '#f472b6', borderDash: null,
		regionOutline: '#f472b6',
		province: '#d4d4d4',
		deptLine: '#93c5fd',
		accent: '#60a5fa',
		compare: '#fbbf24',
		select: SELECT_OUTLINE,
		hillshade: { shadow: '#0a0a1a', highlight: '#8899bb', accent: undefined, exaggeration: 0.7 },
		light: { color: '#e0f0ff', intensity: 0.55 },
	},
	claro: {
		paper: '#f9fafc', water: '#dfe7ec', waterway: '#c9d6de', placeText: '#3a3b3d',
		halo: 'rgba(249,250,252,0.92)', haloWidth: 1.4,
		// a paper veil: the neighbours stay legible but quiet
		mask: '#f9fafc', maskOpacity: 0.7,
		territoryBg: '#d3d5d6', territoryBgOpacity: 0.5,
		border: '#565759', borderDash: [4, 2],
		regionOutline: '#565759',
		province: '#8a8b8d',
		deptLine: '#8a8b8d',
		accent: '#2f6f9f',
		compare: '#b7791f',
		select: '#1c1c1c',
		hillshade: { shadow: '#858688', highlight: '#ffffff', accent: '#b3b4b6', exaggeration: 0.35 },
		light: { color: '#f7f9fb', intensity: 0.7 },
	},
};

export const FONT = {
	sans: "'IBM Plex Sans', system-ui, -apple-system, 'Segoe UI', sans-serif",
	serif: "'Newsreader', Georgia, 'Times New Roman', serif",
	mono: "'IBM Plex Mono', ui-monospace, 'Cascadia Mono', Consolas, monospace",
} as const;

// Hex values for MapLibre (dark/screen variants, same as :root in app.css).
export const SELLO_HEX = {
	cat: ['#c4572f', '#3d7fc4', '#2a9468', '#b5871c'],
	seq: ['#74301f', '#9b4126', '#c4572f', '#de7a4f', '#f1a483'],
	div: ['#7fb0e6', '#3d7fc4', '#35659f', '#5c6370', '#8f3f24', '#c4572f', '#ea9070'],
	ink: ['#e8e6e3', '#b9bec6', '#9aa1ab'],
	surface: '#0b0d14',
} as const;

// Moran «Cuadrantes» by class code (MoranPanel toggleLisa): 1 LL río · 2 LH light
// río · 3 HL light tierra · 4 HH tierra. LISA for the chart (CSS vars, print
// swaps them), LISA_HEX for the map (= :root --s-lisa-*).
export const LISA = ['var(--s-lisa-1)', 'var(--s-lisa-2)', 'var(--s-lisa-3)', 'var(--s-lisa-4)'] as const;
export const LISA_HEX: Record<number, string> = { 1: '#3d7fc4', 2: '#7fb0e6', 3: '#ea9070', 4: '#c4572f' };

// 3×3 bivariate: tierra (X) × río (Y). Index = class − 1 with
// class = 1 + yTercile·3 + xTercile (rows low Y → high Y, X grows to the right).
// Dark map (BIVAR_HEX): additive, like light on the dark map — low-low is a dark
// neutral that recedes, each axis brightens in its hue and high-high is the light
// mix. The first grid (= BIVAR_HEX_LIGHT) had light poles: low-low #f2f0ed was the
// brightest class on #0e1117, and with zero-inflated layers (flood: ~98 % of
// hexagons at 0 fall in "low") the map read as a white sheet (fixed 1-nov-2026).
// Validated --pairs all as drawn (fill 0.78 over #0e1117 and over the gray
// territory backdrop #29303d): CVD worst ΔE 7.7 both (light grid: 7.1 / 6.9),
// normal-vision 8.8 / 8.6 (9.0 / 8.9), the 8 non-low-low classes ≥ 2.2:1 drawn
// and ≥ 3:1 at full opacity; low-low 1.5 / 1.2:1, present but quiet.
// No 3×3 reaches the 15 normal-vision floor: the legend grid stays next to the map.
export const BIVAR_HEX = [
	'#3c3f44', '#9a6b56', '#ef864e',
	'#3e799e', '#9b9ba5', '#f7bda9',
	'#2da7e6', '#a4cbee', '#fae4e8',
] as const;
// Light grid (multiplicative, light poles): paper / print now, the Claro theme later.
export const BIVAR_HEX_LIGHT = [
	'#f2f0ed', '#efb39b', '#e8825a',
	'#b3d0ef', '#b19b9c', '#ab705b',
	'#78afe8', '#768298', '#735d58',
] as const;
// Hexagons without a Y value on the bivariate map: darker than low-low and quiet
// (the generic no-data gray #6b7280 out-shone most classes, and with a census Y
// most rural hexagons have no Y).
export const BIVAR_NODATA = '#23272e';
// Charts and legend read the CSS vars (print swaps them to the light grid).
export const BIVAR = Array.from({ length: 9 }, (_, i) => `var(--s-bivar-${i + 1})`);

// ── Map ramps (S4) ─────────────────────────────────────────────────────────────
// Validated against the dark basemap (#0e1117). On the dark map "more" is
// brighter: low steps recede, high steps carry the eye.
// Risk and pressure (flood, census deprivation, isolation, PM2.5, deforestation,
// EUDR): tierra, one hue.
export const MAP_TIERRA = ['#8a3a22', '#b24c29', '#d0623a', '#e5835a', '#f3ab8c'] as const;
// Aptitude and cover (agriculture, forestry, tree cover): selva, one hue, pale
// to deep: the greenest hexagon is the one with the most cover or aptitude (the
// first version had high = pale mint, which read backwards; fixed 31-oct-2026).
export const MAP_SELVA = ['#c9dccf', '#9ccbb0', '#6bb88f', '#3fa06f', '#1f8455', '#136b43'] as const;
// Change (delta mode), left → right: worse (tierra) · no change (gris) · better
// (río). Danger layers flip the delta sign upstream so an increase lands left.
export const MAP_DIV = ['#ea9070', '#c4572f', '#8f3f24', '#5c6370', '#35659f', '#3d7fc4', '#7fb0e6'] as const;

/** Colour at t ∈ [0, 1] along evenly spaced stops (linear RGB blend between neighbours). */
export function rampColor(stops: readonly string[], t: number): string {
	const x = Math.max(0, Math.min(1, Number.isFinite(t) ? t : 0)) * (stops.length - 1);
	const i = Math.min(stops.length - 2, Math.floor(x));
	const f = x - i;
	const a = hexRgb(stops[i]), b = hexRgb(stops[i + 1]);
	return `rgb(${Math.round(a[0] + (b[0] - a[0]) * f)},${Math.round(a[1] + (b[1] - a[1]) * f)},${Math.round(a[2] + (b[2] - a[2]) * f)})`;
}

/** Diverging colour for a signed value: −1 → MAP_DIV[0] (worse), 0 → gris, +1 → MAP_DIV[6]. */
export function divColor(t: number): string {
	return rampColor(MAP_DIV, (Math.max(-1, Math.min(1, t)) + 1) / 2);
}

export const rampGradient = (stops: readonly string[]): string => `linear-gradient(to right, ${stops.join(', ')})`;

function hexRgb(h: string): [number, number, number] {
	return [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)];
}

// Fixed classes for hazards (flood, deforestation, PM2.5 bands): tierra → oro,
// lightness rising with the class, so on the dark map the worst class is the
// brightest. Two hues on purpose (one-hue tierra classes lost the class-to-class
// contrast of the old traffic light). Validated on #0e1117: 3 and 4 classes pass
// CVD and normal vision (all pairs ≥ 15); 5 classes: adjacent ΔE 12 normal, so
// they are always labelled (Flow bands carry their names).
export const MAP_CLASS_3 = ['#a2452a', '#df7440', '#f2c46b'] as const;
export const MAP_CLASS_4 = ['#8a3a22', '#d0602f', '#efa046', '#fbe3a6'] as const;
export const MAP_CLASS_5 = ['#7a3120', '#b84e2b', '#e2793b', '#eeac52', '#fbe6ae'] as const;
// Travel time (isolation): near is a dark, low-chroma brown that recedes, far
// climbs tierra → oro to the brightest class. MAP_TIERRA read backwards here:
// its dark, saturated red sat on the capital and its pale end on the most
// isolated places, and a linear min–max domain left whole departments one flat
// tone (fixed 1-nov-2026). Fitted in OKLCH, validated on #0e1117
// (validate_palette): L monotone with ΔL ≥ 0.06, darkest step 2.04:1, adjacent
// ΔE normal ≥ 14.2 (MAP_CLASS_5: 12.2), CVD worst 10.0 deutan. The near step
// keeps ΔE 16 from the no-data gray #6b7280 (Chaco, Formosa and the Paraguayan
// Chaco have 3–14 % of cells without a time) and 11 from the backdrop #29303d.
export const MAP_ACCESS_5 = ['#5a4234', '#c0502b', '#f97408', '#f0b64f', '#f5e8ba'] as const;
// Density bands match the map's viridis ramp (Map.computeHexColor default branch).
export const MAP_VIRIDIS_3 = ['#5b21b6', '#21918c', '#fde725'] as const;

// ── Data colors on the Claro map (atlas sheet) ─────────────────────────────────
// Designed for paper, not the dark set inverted: "more" is darker (print convention,
// as the @media print tokens), same hues and meanings as the dark set (tierra = risk,
// selva = aptitude/cover, río · gris · tierra = change, tierra → oro classes).
// Generated in OKLCH so that the color AS DRAWN hits its target: hexagons at 0.88
// (HEX_OPACITY claro: at 0.78 black only reaches L 0.40 on paper and every ramp
// washed out) over territory-bg #d9d4cb @0.5 over paper #fbfaf7 = #eae7e1 (warm paper
// until 4-nov-2026; now #d3d5d6 over #f9fafc = #e6e8e9, same OKLCH L, neutral. Re-measured
// in OKLab ×100 against it: zero ↔ backdrop 5.5 and nodata ↔ zero 19.5 (unchanged),
// USO_CLARO ↔ backdrop ≥ 13.6 (13.5), CLASS_5_CLARO ≥ 7.5 (6.7); the figures below are
// the 1-nov validation).
// Validated drawn with dataviz validate_palette (1-nov-2026):
// - ramps (--ordinal): monotone L, adjacent ΔL ≥ 0.06, hue spread ≤ 22° on the
//   one-hue ramps. The light end sits at 1.2–1.5:1 on purpose (the 2:1 floor
//   FAILs): on paper the low class holds most hexagons and must recede (El Soberbio
//   lesson: the majority class cannot carry a strong color). Chroma, not contrast,
//   parts it from the backdrop and from the gray zero/no-data.
// - classes (--pairs all): CLASS_3 normal 18.5 / CVD 18.1; CLASS_4 14.7 / 13.2;
//   CLASS_5 12.6 / 11.0 and ACCESS_5 12.0 / 10.5 (dark: 12.2 and 14.2; their
//   classes are always labelled); AGRI 16.6 / 10.7; LISA 19.2 / 16.1.
// - BIVAR_HEX_LIGHT keeps its print validation (normal 9.0, CVD 7.1: the legend
//   grid stays next to the map, as in the dark).
export const MAP_TIERRA_CLARO = ['#dbad90', '#cf7d59', '#bb4f2e', '#952b12', '#641608'] as const;
export const MAP_SELVA_CLARO = ['#a6caad', '#75b286', '#459865', '#1c7b4c', '#025d36', '#004327'] as const;
// Neutral magnitude (density, the dark map's viridis): río, one hue.
export const MAP_RIO_CLARO = ['#aec6dd', '#7da4c9', '#4b81b9', '#1f5ea2', '#0b3c79'] as const;
// Night lights and economic activity (dark: a night-sky ramp that glows): on paper
// a magnitude like any other, oro → tierra.
export const MAP_ORO_CLARO = ['#e3cc93', '#d89e4a', '#c56a1b', '#a33c11', '#6b1d0c'] as const;
export const MAP_DIV_CLARO = ['#6f1908', '#af4424', '#ce937a', '#b9b8b5', '#88a6c7', '#3068a6', '#083975'] as const;
export const LISA_HEX_CLARO: Record<number, string> = { 1: '#0a4a8c', 2: '#6ea5da', 3: '#de8756', 4: '#952c10' };
export const MAP_CLASS_3_CLARO = ['#e0b972', '#c55c13', '#7a230e'] as const;
export const MAP_CLASS_4_CLARO = ['#e4d197', '#df8021', '#b63f03', '#731c0a'] as const;
export const MAP_CLASS_5_CLARO = ['#e5d7a6', '#ec9c3e', '#d75f0e', '#a63410', '#67190c'] as const;
export const MAP_ACCESS_5_CLARO = ['#dad0c2', '#dc9346', '#cb5400', '#9d2c07', '#5c160d'] as const;
// Crop suitability S1 · S2 · S3 · N (config AGRI_APT_CLASSES holds the dark set).
export const AGRI_APT_HEX = ['#1f7a52', '#86c49c', '#d4a53c', '#b4482a'] as const;
export const AGRI_APT_CLARO = ['#257848', '#94c79e', '#cc841c', '#731402'] as const;
// Land use (MapBiomas AR Col. 3), nominal: monte · plantación · yerba/té y perennes · cultivo anual y pastura ·
// pastizal y arbustal · estero · agua · urbano · mosaico (no class reaches half the cell: a neutral, the «other»).
// 8 hues at most (dataviz rule). Searched in OKLCH with fixed hue intents and validated AS DRAWN with
// validate_palette --pairs all (3-nov-2026): dark fill 0.78 over territory-bg → normal ΔE 17.5, CVD 9.4;
// claro fill 0.88 over paper → 17.1 / 9.0; «sin dato» ≥ 10 and the backdrop ≥ 8 from every class. The
// lightness-band/chroma FAILs are the neutral mosaico (by design) and the map's own fill, as in CLASS_5.
// The 1985 → 2025 trajectory reuses these colors (its classes are named by where the cell ended).
export const USO_HEX = ['#006747', '#4c0354', '#bb5ab1', '#d7cf04', '#a88849', '#2dc3d4', '#1d49b6', '#bc1400', '#eaeaea'] as const;
export const USO_CLARO = ['#095a39', '#48245f', '#e571dc', '#fec306', '#9e6a2f', '#00988a', '#2561bb', '#b00002', '#b8b5b4'] as const;
// Harvest situation (forestry_harvest), 4 steps toward the rotation + loss + no data. Index 3 = «more» in both
// themes. Validated as drawn (fill 0.85 over the dark basemap, 0.9 over paper) with the USO_HEX blue and the nodata
// gray, --pairs all (3-nov-2026): dark normal ΔE 15.6, CVD 10.7; claro 15.0 / 12.7. MAP_CLASS_4 gave 14.4 on dark.
export const HARVEST_HEX = ['#7a2e1b', '#cc5a26', '#f2a63f', '#fde9b8'] as const;
export const HARVEST_CLARO = ['#e4d197', '#df8021', '#b63f03', '#6e1a0a'] as const;
// A measured 0 (zero class) and no reading: quiet grays, zero the lighter one.
export const ZERO_HEX = '#1e293b';
// Claro: the measured 0 stays a light warm gray, ΔE2000 4 from the territory
// backdrop as drawn (paper + territoryBg 0.5, hex fill 0.88). A near-paper 0
// melted into it (ΔE 0.2–1.9: «no water» read as «no hexagon»), so the
// separation from «no data» comes from a darker nodata (2-nov-2026).
export const ZERO_CLARO = '#d2d4d5';
// «Sin dato» as a fixed class (macizos without planting year or cited rotation): the map palette's nodata gray.
export const NODATA_HEX = '#6b7280';

export interface MapPalette {
	tierra: readonly string[]; selva: readonly string[]; div: readonly string[];
	seq: readonly string[];        // neutral magnitude ('sequential')
	warm: readonly string[]; night: readonly string[];
	lisa: Record<number, string>;
	bivar: readonly string[]; bivarNodata: string;
	nodata: string;
	hexLine: string;               // hairline between hexagons
}
export const MAP_PALETTE: Record<MapTema, MapPalette> = {
	oscuro: {
		tierra: MAP_TIERRA, selva: MAP_SELVA, div: MAP_DIV,
		seq: ['#5b21b6', '#21918c', '#fde725'],
		warm: ['#78350f', '#f59e0b', '#fde725'],
		night: ['#0b1026', '#f59e0b', '#fff7d6'],
		lisa: LISA_HEX,
		bivar: BIVAR_HEX, bivarNodata: BIVAR_NODATA,
		nodata: '#6b7280',
		hexLine: '#374151',
	},
	claro: {
		tierra: MAP_TIERRA_CLARO, selva: MAP_SELVA_CLARO, div: MAP_DIV_CLARO,
		seq: MAP_RIO_CLARO,
		warm: MAP_ORO_CLARO, night: MAP_ORO_CLARO,
		lisa: LISA_HEX_CLARO,
		bivar: BIVAR_HEX_LIGHT, bivarNodata: '#c9cacc',
		// ΔE2000 17 from ZERO_CLARO as drawn (was #b1b1ae: 7.7, the two grays the
		// texts tell apart looked alike); neutral, so it never reads as a class.
		nodata: '#8c8d90',
		hexLine: '#f9fafc',        // a paper gap, not a dark rule
	},
};

// Fixed classes (config.ts ColorClasses) carry the dark hex; classColor() gives
// the Claro counterpart, pair by pair, so the class tables stay one list.
const CLASS_PAIRS: Array<[readonly string[], readonly string[]]> = [
	[MAP_CLASS_3, MAP_CLASS_3_CLARO], [MAP_CLASS_4, MAP_CLASS_4_CLARO], [MAP_CLASS_5, MAP_CLASS_5_CLARO],
	[MAP_ACCESS_5, MAP_ACCESS_5_CLARO], [MAP_VIRIDIS_3, [MAP_RIO_CLARO[0], MAP_RIO_CLARO[2], MAP_RIO_CLARO[4]]],
	[AGRI_APT_HEX, AGRI_APT_CLARO], [[ZERO_HEX], [ZERO_CLARO]],
	[USO_HEX, USO_CLARO], [MAP_SELVA, MAP_SELVA_CLARO], [[NODATA_HEX], ['#8c8d90']], [HARVEST_HEX, HARVEST_CLARO],
];
const CLARO_OF = new Map<string, string>();
for (const [dark, light] of CLASS_PAIRS) dark.forEach((c, i) => {
	const k = c.toLowerCase(), prev = CLARO_OF.get(k);
	if (prev && prev !== light[i]) throw new Error(`sello: ${c} has two Claro counterparts`);
	CLARO_OF.set(k, light[i]);
});
/** The color of a fixed class on the given theme (dark hex in, same hex on oscuro). */
export function classColor(c: string, t: MapTema): string {
	return t === 'claro' ? CLARO_OF.get(c.toLowerCase()) ?? c : c;
}

// 3D buildings by built volume (m³), Claro: dark ink, a house mid gray, a tower
// almost black (variant D, chosen by the author on 1-nov over A pale warm gray, B
// uniform charcoal without the volume, C tierra, which competes with the risk
// layers). The dark set is config COLOR_RAMPS.volume. Same breaks.
export const BUILDING_VOLUME_CLARO = ['#8e8881', '#746e67', '#5b554f', '#423d37', '#2b2621', '#17130f', '#070401'] as const;

/** Readable ink on a coloured fill (labels drawn on bands, swatches). */
export function inkOn(hex: string): string {
	const [r, g, b] = hexRgb(hex).map(c => {
		const s = c / 255;
		return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
	});
	return 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.22 ? 'rgba(0,0,0,0.78)' : 'rgba(255,255,255,0.92)';
}
