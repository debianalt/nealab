import { i18n } from '$lib/stores/i18n.svelte';

// Display form of a variable's `unit`. The config keeps one canonical unit string
// per variable (used for logic, e.g. `unit === 'percentil'`); only the units that
// carry words are localised here. Physical symbols (µg/m³, min, km, mm, °, °C·d,
// nW/cm²/sr, g/kg, %, /100, pctl) are language-neutral and pass through.
const UNIT_LABELS: Record<string, Record<string, string>> = {
	'%/yr':     { es: '%/año',     en: '%/yr',           gn: '%/ary',    pt: '%/ano' },
	'percentil': { es: 'percentil', en: 'percentile',     gn: 'percentil', pt: 'percentil' },
	'meses':    { es: 'meses',     en: 'months',         gn: 'jasy',     pt: 'meses' },
	'hab/km²':  { es: 'hab/km²',   en: 'inhab/km²',      gn: 'hab/km²',  pt: 'hab/km²' },
	'viv/km²':  { es: 'viv/km²',   en: 'dwellings/km²',  gn: 'óga/km²',  pt: 'dom/km²' },
	'edif/km²': { es: 'edif/km²',  en: 'bldg/km²',       gn: 'óga/km²',  pt: 'edif/km²' },
};

export function unitLabel(u: string | null | undefined): string {
	if (!u) return '';
	const entry = UNIT_LABELS[u];
	return entry ? (entry[i18n.locale] ?? entry.en) : u;
}
