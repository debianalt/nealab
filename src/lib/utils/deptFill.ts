import type { DeptFill } from '$lib/stores/hex.svelte';
import { SELLO_HEX, type MapTema } from '$lib/utils/sello';

// Province view: departments painted with the active ranking reading, on the
// bars' own linear scale (|value| / max). Keep in sync with the ranking bars
// (OvertureAnalysis: --s-rank, río below zero on signed readings).
//
// Oscuro: tierra (río below zero on signed readings: the Sello's diverging pair,
// not red/green, which collapses under deuteranopia), "more" = more opaque.
// Claro (4-nov-2026): solid classes, the opacity is fixed. A one-color opacity
// ramp read whatever lay under it, and the low classes melted together; tierra also
// means "risk" in the Sello, wrong for a ranking of something good. Unsigned = a
// neutral gray ramp, signed = río below zero and the same grays above (no tierra);
// no data = hatched, because the ramp's middle gray IS the nodata gray.
export interface FillClass { color: string; opacity: number; lo: number; hi: number }   // lo/hi: fraction of max
export interface FillNA { color: string; opacity: number; hatch?: boolean }

const RED = SELLO_HEX.cat[0], GREEN = SELLO_HEX.cat[1];   // tierra, río
export const FILL_NA: FillNA = { color: '#64748b', opacity: 0.18 };
// Hatch stroke: the Claro map's nodata gray (MAP_PALETTE.claro.nodata).
export const FILL_NA_CLARO: FillNA = { color: '#8c8d90', opacity: 1, hatch: true };

const UNSIGNED: FillClass[] = [0.12, 0.26, 0.4, 0.55, 0.7].map((opacity, i) => ({ color: RED, opacity, lo: i / 5, hi: (i + 1) / 5 }));
const SIGNED: FillClass[] = [
	{ color: GREEN, opacity: 0.7, lo: -1, hi: -0.7 },
	{ color: GREEN, opacity: 0.42, lo: -0.7, hi: -0.4 },
	{ color: GREEN, opacity: 0.2, lo: -0.4, hi: -0.1 },
	{ color: '#cbd5e1', opacity: 0.16, lo: -0.1, hi: 0.1 },   // about zero
	{ color: RED, opacity: 0.2, lo: 0.1, hi: 0.4 },
	{ color: RED, opacity: 0.42, lo: 0.4, hi: 0.7 },
	{ color: RED, opacity: 0.7, lo: 0.7, hi: 1 },
];

// Neutral gray, OKLCH C 0.003 (h 250), drawn at 0.85 over paper #f9fafc to
// L 0.86 · 0.75 · 0.64 · 0.53 · 0.42 (#cfd1d3 #acaeb0 #8b8c8e #6a6c6d #4c4d4f):
// adjacent ΔE 11.0, paper → first class 12.5. validate_palette --ordinal passes
// monotone L and ΔL; the light end sits at 1.47:1 on purpose (2:1 FAILs), as in the
// other Claro ramps: the low class often holds most departments and must recede.
export const RANK_GRAY_CLARO = ['#c8cacc', '#9ea1a3', '#78797b', '#515354', '#2d2e30'] as const;
const CLARO_OPACITY = 0.85;
const UNSIGNED_CLARO: FillClass[] = RANK_GRAY_CLARO.map((color, i) => ({ color, opacity: CLARO_OPACITY, lo: i / 5, hi: (i + 1) / 5 }));
// Signed, −1 → +1: río (decline) · neutral ≈ 0 · gray (growth, as the unsigned ramp:
// darker = more). Drawn at 0.85 over #f9fafc: #254a7b #467bb0 #89afd6 | #dee0e1 |
// #a9abad #797a7c #4c4d4f. validate_palette (4-nov-2026): adjacent ΔE 15.9–17.8 normal
// and CVD (both floors PASS), ≈ 0 at 7.9 from the paper; each arm passes --ordinal
// except the light-end contrast, which is the ≈ 0 class by design.
export const RANK_SIGNED_CLARO = ['#002b64', '#2665a3', '#75a2cf', '#d9dbdc', '#9b9d9f', '#626365', '#2d2e30'] as const;
const SIGNED_CLARO: FillClass[] = SIGNED.map((c, i) => ({ ...c, color: RANK_SIGNED_CLARO[i], opacity: CLARO_OPACITY }));

export function deptFillClasses(signed: boolean, tema: MapTema = 'oscuro'): FillClass[] {
	if (tema === 'claro') return signed ? SIGNED_CLARO : UNSIGNED_CLARO;
	return signed ? SIGNED : UNSIGNED;
}

export function deptFillNA(tema: MapTema = 'oscuro'): FillNA {
	return tema === 'claro' ? FILL_NA_CLARO : FILL_NA;
}

export function deptFillStyle(v: number | null | undefined, f: DeptFill, tema: MapTema = 'oscuro'): FillNA {
	if (v == null || !Number.isFinite(v)) return deptFillNA(tema);
	const r = Math.max(-1, Math.min(1, v / f.max));
	const classes = deptFillClasses(f.signed, tema);
	const c = classes.find(c => r < c.hi) ?? classes[classes.length - 1];
	return { color: c.color, opacity: c.opacity };
}
