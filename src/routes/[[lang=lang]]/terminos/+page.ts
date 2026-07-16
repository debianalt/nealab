// Without this file the route falls through to the SPA fallback, so crawlers got an
// HTTP 404 for a URL that humans can reach. Prerendered it serves a real 200; the
// noindex still comes from <Seo noindex /> in +page.svelte.
import { LANG_ENTRIES } from '$lib/utils/locale-path';

export const ssr = true;
export const prerender = true;

// noindex, so these variants are UX-only — a pt reader following "Leer términos"
// should not land in Spanish.
export function entries() {
	return LANG_ENTRIES;
}
