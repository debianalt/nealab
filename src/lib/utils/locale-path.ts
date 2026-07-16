import type { Locale } from '$lib/stores/i18n.svelte';

export const LOCALES: readonly Locale[] = ['es', 'en', 'gn', 'pt'];

// Spanish lives on the unprefixed path, so it never appears as a URL segment.
export const PREFIXED_LOCALES: readonly Locale[] = ['en', 'gn', 'pt'];

const PREFIX_RE = /^\/(en|gn|pt)(?=\/|$)/;

/** Prerender entries for every route under [[lang=lang]]. `{}` resolves to the
 *  unprefixed (Spanish) path — see resolve_route in @sveltejs/kit. */
export const LANG_ENTRIES: Array<Record<string, string>> = [
	{},
	...PREFIXED_LOCALES.map((lang) => ({ lang })),
];

/** `/en/servicios` → `/servicios`. Leaves unprefixed paths alone. */
export function stripLocale(pathname: string): string {
	return pathname.replace(PREFIX_RE, '') || '/';
}

/**
 * Prefixes an internal path with `locale`.
 *
 * Only for text-route targets. The map at `/` is not locale-prefixed — it carries
 * locale in localStorage — so never pass `/` or `/?a=…` through here.
 */
export function lp(path: string, locale: Locale): string {
	return locale === 'es' ? path : `/${locale}${path}`;
}
