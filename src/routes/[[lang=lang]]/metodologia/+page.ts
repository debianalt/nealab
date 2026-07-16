import { listMethodologyIds } from '$lib/content/methodology';
import { ANALYSIS_REGISTRY, LENS_CONFIG } from '$lib/config';
import type { LensId } from '$lib/config';
import { LANG_ENTRIES } from '$lib/utils/locale-path';
import type { Locale } from '$lib/stores/i18n.svelte';

export const ssr = true;
export const prerender = true;

export function entries() {
	return LANG_ENTRIES;
}

export function load({ params }: { params: { lang?: string } }) {
	const locale = (params.lang ?? 'es') as Locale;
	const ids = new Set(listMethodologyIds());

	const byLens = (Object.keys(LENS_CONFIG) as LensId[]).map((lensId) => ({
		lensId,
		color: LENS_CONFIG[lensId].color,
		// Was hardcoded to `.es`, which only went unnoticed while every locale shared
		// this one URL. /pt/metodologia would have shown Spanish lens labels.
		label: LENS_CONFIG[lensId].label[locale] ?? LENS_CONFIG[lensId].label.es,
		analyses: ANALYSIS_REGISTRY.filter(
			(a) => a.lensId === lensId && ids.has(a.id) && a.status === 'available'
		).map((a) => ({ id: a.id, titleKey: a.titleKey, descKey: a.descKey }))
	})).filter((g) => g.analyses.length > 0);

	return { byLens };
}
