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
// What the reader picked (chart brush, the hexagon in the ficha): tierra in the
// chart and the same tierra outline on the map.
export const SELECT = TIERRA;
export const SURFACE = 'var(--s-surface)';

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

// 3×3 bivariate: tierra (X) × río (Y), multiplicative mix with light poles so the
// high-high class still clears 3:1 on the dark map. Index = class − 1 with
// class = 1 + yTercile·3 + xTercile (rows low Y → high Y, X grows to the right).
// Validated --pairs all on #0e1117: CVD worst ΔE 8.8 (Stevens' grid: 2.2),
// normal-vision worst 11.1 (Stevens: 7.6), all 9 ≥ 3:1 (Stevens: one at 2.3).
// No 3×3 reaches the 15 normal-vision floor across all pairs: the legend grid
// stays next to the map.
export const BIVAR_HEX = [
	'#f2f0ed', '#efb39b', '#e8825a',
	'#b3d0ef', '#b19b9c', '#ab705b',
	'#78afe8', '#768298', '#735d58',
] as const;

// ── Map ramps (S4) ─────────────────────────────────────────────────────────────
// Validated against the dark basemap (#0e1117). On the dark map "more" is
// brighter: low steps recede, high steps carry the eye.
// Risk and pressure (flood, census deprivation, isolation, PM2.5, deforestation,
// EUDR): tierra, one hue.
export const MAP_TIERRA = ['#8a3a22', '#b24c29', '#d0623a', '#e5835a', '#f3ab8c'] as const;
// Aptitude and cover (agriculture, forestry, tree cover): selva, one hue.
export const MAP_SELVA = ['#1a5a40', '#217a55', '#2a9468', '#4fb184', '#86cda6', '#c0e6cf'] as const;
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
