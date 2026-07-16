import { i18n, type Locale } from '$lib/stores/i18n.svelte';

/**
 * On these routes the URL is the only source of truth for locale. If localStorage could
 * override it, /servicios would render English for some visitors while its hreflang
 * claims Spanish.
 *
 * The singleton is set here rather than in a component because this is a *universal*
 * load: it runs on the server during prerendering — where $effect never fires — and
 * again on the client, re-running whenever `params` changes, which covers switching
 * locale via LangSwitcher. Both paths complete before anything renders, so every
 * consumer of `i18n.locale` (SERVICIOS[i18n.locale], the ~250 t() calls) resolves to
 * the URL's language with no param threaded through the app and no post-render flash.
 *
 * setLocale also persists, so returning to the map keeps the language. Its localStorage
 * write is guarded, so this is a no-op field assignment on the server.
 *
 * Mutating a module singleton during SSR would leak across requests on a real server;
 * here prerendering is sequential and single-process, and adapter-static means there is
 * no SSR server at all.
 */
export function load({ params }: { params: { lang?: string } }) {
	// No `lang` param means the unprefixed path, which is Spanish (see src/params/lang.ts).
	const lang = (params.lang ?? 'es') as Locale;
	i18n.setLocale(lang);
	return { lang };
}
