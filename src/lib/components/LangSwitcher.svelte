<script lang="ts">
	// Links, not buttons: on [[lang=lang]] routes the URL carries the locale, so
	// switching language is a navigation. The map has its own inline switcher that
	// still mutates i18n directly — it lives outside [[lang]] and has no URL to go to.
	import { page } from '$app/state';
	import { LOCALES, lp, stripLocale } from '$lib/utils/locale-path';
	import type { Locale } from '$lib/stores/i18n.svelte';

	const basePath = $derived(stripLocale(page.url.pathname));
	const current = $derived((page.params.lang ?? 'es') as Locale);
</script>

<div class="lang-switcher">
	{#each LOCALES as lang}
		<a class:active={current === lang} href={lp(basePath, lang)} hreflang={lang}>
			{lang === 'pt' ? 'BR' : lang.toUpperCase()}
		</a>
	{/each}
</div>

<style>
	.lang-switcher {
		display: flex;
		align-items: center;
		gap: 2px;
	}
	.lang-switcher a {
		padding: 4px 8px;
		font-size: 10px;
		font-weight: 600;
		letter-spacing: 0.05em;
		background: transparent;
		border: 1px solid transparent;
		border-radius: 4px;
		color: rgba(255, 255, 255, 0.4);
		cursor: pointer;
		font-family: inherit;
		text-decoration: none;
		transition: all 0.15s;
	}
	.lang-switcher a:hover {
		color: rgba(255, 255, 255, 0.8);
	}
	.lang-switcher a.active {
		background: rgba(255, 255, 255, 0.08);
		border-color: rgba(255, 255, 255, 0.2);
		color: rgba(255, 255, 255, 0.9);
	}
</style>
