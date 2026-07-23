import { LANG_ENTRIES } from '$lib/utils/locale-path';

// ssr=true is required so <Seo> prerenders into %sveltekit.head% (which sits above
// app.html's fallback tags). Do not copy the map's ssr=false.
export const ssr = true;
export const prerender = true;

export function entries() {
	return LANG_ENTRIES;
}
