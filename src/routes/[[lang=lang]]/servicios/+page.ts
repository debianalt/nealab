import { LANG_ENTRIES } from '$lib/utils/locale-path';

export const ssr = true;
export const prerender = true;

// Explicit rather than leaning on the crawler: SvelteKit would discover the prefixed
// variants by following LangSwitcher's links, but then prerender coverage would hinge
// on a component's markup and drop silently if it changed.
export function entries() {
	return LANG_ENTRIES;
}
