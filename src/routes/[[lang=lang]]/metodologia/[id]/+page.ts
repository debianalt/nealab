import { error } from '@sveltejs/kit';
import { getMethodologyContent, listMethodologyIds } from '$lib/content/methodology';
import { HEX_LAYER_REGISTRY, getAnalysisById } from '$lib/config';
import { LANG_ENTRIES } from '$lib/utils/locale-path';

export const ssr = true;
export const prerender = true;

// Cross product: 4 locales × 17 analyses = 68 pages.
export function entries() {
	return LANG_ENTRIES.flatMap((lang) => listMethodologyIds().map((id) => ({ ...lang, id })));
}

export function load({ params }: { params: { id: string } }) {
	const content = getMethodologyContent(params.id);
	if (!content) throw error(404, `Metodología no encontrada: ${params.id}`);

	const layerCfg = HEX_LAYER_REGISTRY[params.id];
	const analysis = getAnalysisById(params.id);

	return {
		id: params.id,
		content,
		titleKey: layerCfg?.titleKey ?? analysis?.titleKey ?? params.id,
		descKey: analysis?.descKey,
		variables: layerCfg?.variables ?? [],
		colorScale: layerCfg?.colorScale,
	};
}
