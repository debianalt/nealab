<script lang="ts">
	// Per-route metadata. Only renders into the HTML on routes with ssr = true —
	// `/` is ssr=false and falls back to the tags in app.html.
	// og:image / og:type / twitter:card are identical site-wide and live in app.html.
	import { page } from '$app/state';
	import { LOCALES, lp, stripLocale } from '$lib/utils/locale-path';
	import type { Locale } from '$lib/stores/i18n.svelte';

	interface Props {
		title: string;
		description: string;
		/** Defaults to `title` — pass only when the social title differs. */
		ogTitle?: string;
		/** Defaults to `description`. */
		ogDescription?: string;
		noindex?: boolean;
	}

	let { title, description, ogTitle, ogDescription, noindex = false }: Props = $props();

	const url = $derived(`https://spatia.ar${page.url.pathname}`);
	const locale = $derived((page.params.lang ?? 'es') as Locale);

	// Same page, other languages. Google requires every variant to list them all,
	// itself included, and each must use the same URL the others point at.
	const basePath = $derived(stripLocale(page.url.pathname));
	const alternates = $derived(
		LOCALES.map((l) => ({ hreflang: l, href: `https://spatia.ar${lp(basePath, l)}` }))
	);

	const OG_LOCALE: Record<Locale, string> = {
		es: 'es_AR',
		en: 'en_US',
		pt: 'pt_BR',
		gn: 'gn_PY',
	};
</script>

<svelte:head>
	<title>{title}</title>
	<meta name="description" content={description} />
	<link rel="canonical" href={url} />
	<meta property="og:title" content={ogTitle ?? title} />
	<meta property="og:description" content={ogDescription ?? description} />
	<meta property="og:url" content={url} />
	<meta property="og:locale" content={OG_LOCALE[locale]} />
	{#if noindex}
		<meta name="robots" content="noindex" />
		<!-- No alternates: Google drops hreflang on noindex pages, so emitting them
		     would only be noise. -->
	{:else}
		{#each alternates as alt}
			<link rel="alternate" hreflang={alt.hreflang} href={alt.href} />
		{/each}
		<link rel="alternate" hreflang="x-default" href={`https://spatia.ar${basePath}`} />
	{/if}
</svelte:head>
