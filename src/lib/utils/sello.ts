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
