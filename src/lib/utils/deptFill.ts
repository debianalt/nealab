import type { DeptFill } from '$lib/stores/hex.svelte';

// Province view: departments painted with the active ranking reading, on the
// bars' own linear scale (|value| / max) and colors (red, green below zero on
// signed readings). Over the dark basemap "more" reads as more opaque red; a
// yellow-to-red ramp would make the lowest values the brightest.
export interface FillClass { color: string; opacity: number; lo: number; hi: number }   // lo/hi: fraction of max

const RED = '#dc2626', GREEN = '#22c55e';
export const FILL_NA = { color: '#64748b', opacity: 0.18 };

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

export function deptFillClasses(signed: boolean): FillClass[] {
	return signed ? SIGNED : UNSIGNED;
}

export function deptFillStyle(v: number | null | undefined, f: DeptFill): { color: string; opacity: number } {
	if (v == null || !Number.isFinite(v)) return FILL_NA;
	const r = Math.max(-1, Math.min(1, v / f.max));
	const classes = deptFillClasses(f.signed);
	const c = classes.find(c => r < c.hi) ?? classes[classes.length - 1];
	return { color: c.color, opacity: c.opacity };
}
