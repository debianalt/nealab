/**
 * Display form of the `type_label` a layer's parquet carries (3-nov-2026 text audit).
 *
 * - PM2.5 classes are named by WHO 2021 interim target, not by an adjective: "Muy baja
 *   exposición" for ≤ 10 µg/m³ read as safe, while everything above 5 µg/m³ exceeds the
 *   guideline (cuts in pipeline/compute_pm25_drivers.py WHO_BINS).
 * - The pipeline wrote labels without accents ("Apto estandar", "Optimo templado-humedo").
 * - Some k-means typologies were never named: their labels are column names
 *   ("densidad_hab_km2 bajo, nbi alto", "tasa_empleo alto"). Those return null and the
 *   chip is not shown.
 */

type Loc = 'es' | 'en' | 'gn' | 'pt';

const PM25: Record<string, Record<'es' | 'en' | 'pt', string>> = {
	'Muy baja exposicion': { es: '≤ 10 µg/m³ · meta intermedia 4 de la OMS', en: '≤ 10 µg/m³ · WHO interim target 4', pt: '≤ 10 µg/m³ · meta intermediária 4 da OMS' },
	'Baja exposicion': { es: '10–15 µg/m³ · meta intermedia 3 de la OMS', en: '10–15 µg/m³ · WHO interim target 3', pt: '10–15 µg/m³ · meta intermediária 3 da OMS' },
	'Exposicion moderada': { es: '15–25 µg/m³ · meta intermedia 2 de la OMS', en: '15–25 µg/m³ · WHO interim target 2', pt: '15–25 µg/m³ · meta intermediária 2 da OMS' },
	'Alta exposicion': { es: '25–35 µg/m³ · meta intermedia 1 de la OMS', en: '25–35 µg/m³ · WHO interim target 1', pt: '25–35 µg/m³ · meta intermediária 1 da OMS' },
	'Muy alta exposicion': { es: '> 35 µg/m³ · por encima de las metas de la OMS', en: '> 35 µg/m³ · above the WHO targets', pt: '> 35 µg/m³ · acima das metas da OMS' },
};

const ACCENTS: [RegExp, string][] = [
	[/\bestandar\b/g, 'estándar'],
	[/\blimitacion\b/g, 'limitación'],
	[/\bhidrica\b/g, 'hídrica'],
	[/\bOptimo\b/g, 'Óptimo'],
	[/\bhumedo\b/g, 'húmedo'],
	[/\bexposicion\b/gi, 'exposición'],
	[/\bpresion\b/gi, 'presión'],
];

// A column name followed by alto/bajo: "densidad_hab_km2 bajo", "tasa_empleo alto".
const RAW_COLUMN = /\b[a-z0-9]+_[a-z0-9_]+ (alto|bajo)\b/;

export function displayTypeLabel(label: string | null | undefined, locale: string): string | null {
	if (!label) return null;
	const l = (locale === 'gn' ? 'es' : locale) as Exclude<Loc, 'gn'>;
	const pm = PM25[label];
	if (pm) return pm[l] ?? pm.es;
	if (RAW_COLUMN.test(label)) return null;
	let out = label;
	for (const [re, rep] of ACCENTS) out = out.replace(re, rep);
	return out;
}
